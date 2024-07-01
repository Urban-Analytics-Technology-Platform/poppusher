# type: ignore -- too annoying to get Dagster internals to type check
"""
run.py
------

This module can be used to materialise all assets within a job in an
'intelligent' way.

Specifically, this module was written to solve a specific problem. One can run
an _unpartitioned_ job using the Dagster CLI:

    dagster job launch -j <job_name>

However, that doesn't work for _partitioned_ assets. When it reaches the first
partitioned asset, the command will crash. For those you need to do:

    dagster job backfill -j <job_name> --noprompt

but if the partitioned assets depend on unpartitioned ones, the upstream
unpartitioned assets will be run one time for each partition, which is
extremely inefficient.

This module provides a way to sequentially materialise assets within a job in
much the same way as one might do manually via the web UI: that is,
materialising the most upstream asset first, then its reverse dependencies, and
so on. It handles both assets that are unpartitioned as well as those with
dynamic partitions (static partitions are not supported, but popgetter does not
have any such assets, so this is not a problem for now).

To use it, run:

    set -a; source .env; set +a
    python -m popgetter.run job <job_name>

Note that you must set any environment variables that are required for the job
to run successfully _prior_ to running this script. Because `python -m ...`
imports the entire library _before_ running the script, this script _cannot_
automatically source the environment variables in your `.env` file; you have to
do this manually before running the script.

`set -a` is used here to export the environment variables so that the next
command can see them. (Otherwise, the environment variables are immediately
dropped after the `source` command exits.)

This module can also be used to materialise a single asset in exactly the same
way:

    set -a; source .env; set +a
    python -m popgetter.run asset <asset_name>
"""
from __future__ import annotations

import argparse
import time

from dagster import (
    DagsterInstance,
    DynamicPartitionsDefinition,
    materialize,
    AssetsDefinition,
)
from dagster._core.errors import DagsterHomeNotSetError

from . import defs


def find_materialisable_asset_names(dep_list, done_asset_names: set[str]) -> set[str]:
    """Given a dictionary of {node: dependencies} and a set of asset names
    which have already been materialised, return a set of asset names which
    haven't been materialized yet but can now be.

    dep_list should be obtained from
    defs.get_job_def(job_name)._graph_def._dependencies.
    """
    materialisable_asset_names = set()

    for asset, dep_dict in dep_list.items():
        if asset.name in done_asset_names:
            continue

        if all(dep.node in done_asset_names for dep in dep_dict.values()):
            materialisable_asset_names.add(asset.name)

    return materialisable_asset_names


def materialise_asset(asset: AssetsDefinition,
                      upstream_deps: [AssetsDefinition],
                      delay: float,
                      instance: DagsterInstance):
    """Materialise an asset."""
    print(f"Materialising: {asset.node_def.name}")  # noqa: T201

    partitions_def = asset.partitions_def
    if partitions_def is None:
        # Unpartitioned

        # https://docs.dagster.io/_apidocs/execution#dagster.materialize -- note
        # that the `assets` keyword argument needs to include upstream assets as
        # well. We use `selection` to specify the asset that is actually being
        # materialised.
        materialize(
            assets=[asset, *upstream_deps],
            selection=[asset],
            instance=instance,
        )
        time.sleep(delay)

    else:
        # Partitioned
        if type(partitions_def) != DynamicPartitionsDefinition:
            # Everything in popgetter is dynamically partitioned so we
            # should not run into this.
            err = "Non-dynamic partitions not implemented yet"
            raise NotImplementedError(err)
        partition_names = instance.get_dynamic_partitions(partitions_def.name)

        # print(f" \n\n\n - partitions: {partition_names}\n\n\n")  # noqa: T201

        for partition in partition_names:
            print(f"  - with partition key: {partition}")  # noqa: T201
            time.sleep(delay)
            materialize(
                assets=[asset, *upstream_deps],
                selection=[asset],
                partition_key=partition,
                instance=instance,
            )

def get_dagster_instance():
    """Same as DagsterInstance.get(), but with an error message that is more
    specialised to this script."""
    try:
        return DagsterInstance.get()
    except DagsterHomeNotSetError:
        err = (f"$DAGSTER_HOME was not set. Note that running this module"
               f" does not automatically source your .env file: you must"
               f" do this yourself, e.g. with\n\n"
               f"     set -a; source .env; set +a\n\n"
               f" prior to running this module. See the `popgetter.run`"
               f" module docstring for an explanation of this.")
        raise RuntimeError(err) from None

def materialise_asset_from_args(args):
    asset_name, delay = args.asset_name, args.delay
    instance = get_dagster_instance()
    materialise_asset(defs.get_assets_def(asset_name), [], delay, instance)


def run_job(args):
    job_name, delay = args.job_name, args.delay
    instance = get_dagster_instance()

    job = defs.get_job_def(job_name)
    dependency_list = job._graph_def._dependencies
    all_assets = {
        node_handle.name: definition
        for node_handle, definition in job._asset_layer.assets_defs_by_node_handle.items()
    }

    materialised_asset_names = set()
    while len(materialised_asset_names) < len(all_assets):
        asset_names_to_materialise = find_materialisable_asset_names(
            dependency_list, materialised_asset_names
        )

        if len(asset_names_to_materialise) == 0:
            print("No more assets to materialise")  # noqa: T201
            break

        asset_name_to_materialise = asset_names_to_materialise.pop()
        asset_to_materialise = all_assets.get(asset_name_to_materialise)
        upstream_deps = [all_assets.get(k) for k in materialised_asset_names]
        materialise_asset(asset_to_materialise, upstream_deps, delay, instance)

        materialised_asset_names.add(asset_name_to_materialise)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a job (in an intelligent way), or just an asset"
    )

    subparsers = parser.add_subparsers(title="subcommands")
    job_parser = subparsers.add_parser("job", help="Run all assets within a job")
    asset_parser = subparsers.add_parser("asset", help="Run a single asset. The asset cannot have any upstream dependencies.")

    job_parser.add_argument("job_name", type=str, help="Name of the job to run")
    job_parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between materialising successive assets or partitions thereof",
    )
    job_parser.set_defaults(func=run_job)

    asset_parser.add_argument("asset_name", type=str, help="Name of the asset to run")
    asset_parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between materialising successive partitions (if any)",
    )
    asset_parser.set_defaults(func=materialise_asset_from_args)

    args = parser.parse_args()
    args.func(args)
