# type: ignore -- too annoying to get Dagster internals to type check
"""
run_job.py
----------

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

    python -m popgetter.run_job <job_name>

Note that you must set a $DAGSTER_HOME environment variable, and any other
environment variables that are required for the job to run successfully. This
script will source a `.env` file in your working directory, which is similar to
Dagster's behaviour, so you can simply use that file.
"""
from __future__ import annotations

import argparse
import time

from dagster import DagsterInstance, DynamicPartitionsDefinition, materialize
from dotenv import load_dotenv

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


def run_job(job_name: str, delay: float):
    job = defs.get_job_def(job_name)

    # Required for persisting outputs in $DAGSTER_HOME/storage
    instance = DagsterInstance.get()

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

        print(f"Materialising: {asset_name_to_materialise}")  # noqa: T201

        partitions_def = asset_to_materialise.partitions_def

        if partitions_def is None:
            # Unpartitioned

            # https://docs.dagster.io/_apidocs/execution#dagster.materialize -- note
            # that the `assets` keyword argument needs to include upstream assets as
            # well. We use `selection` to specify the asset that is actually being
            # materialised.
            materialize(
                assets=[
                    asset_to_materialise,
                    *(all_assets.get(k) for k in materialised_asset_names),
                ],
                selection=[asset_to_materialise],
                instance=instance,
            )
            time.sleep(delay)
            materialised_asset_names.add(asset_name_to_materialise)

        else:
            # Partitioned
            if type(partitions_def) != DynamicPartitionsDefinition:
                # Everything in popgetter is dynamically partitioned so we
                # should not run into this.
                err = "Non-dynamic partitions not implemented yet"
                raise NotImplementedError(err)
            partition_names = instance.get_dynamic_partitions(partitions_def.name)

            for partition in partition_names:
                print(f"  - with partition key: {partition}")  # noqa: T201
                time.sleep(delay)
                materialize(
                    assets=[
                        asset_to_materialise,
                        *(all_assets.get(k) for k in materialised_asset_names),
                    ],
                    selection=[asset_to_materialise],
                    partition_key=partition,
                    instance=instance,
                )
            materialised_asset_names.add(asset_name_to_materialise)


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run a job (in an intelligent way)")
    parser.add_argument("job_name", type=str, help="Name of the job to run")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between materialising successive assets",
    )
    args = parser.parse_args()
    run_job(args.job_name, args.delay)
