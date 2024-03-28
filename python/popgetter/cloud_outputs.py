from __future__ import annotations

from pathlib import Path

from dagster import (
    AssetOut,
    AssetSelection,
    EnvVar,
    Noneable,
    Output,
    RunConfig,
    RunRequest,
    SkipReason,
    define_asset_job,
    load_assets_from_current_module,
    local_file_manager,
    multi_asset,
    multi_asset_sensor,
)
from dagster import Config as DagsterConfig
from dagster_azure.adls2 import adls2_file_manager
from icecream import ic

resources = {
    "DEV": {"publishing_file_manager": local_file_manager},
    "PRODUCTION": {
        "publishing_file_manager": adls2_file_manager
        # See https://docs.dagster.io/_apidocs/libraries/dagster-azure#dagster_azure.adls2.adls2_file_manager
        # storage_account="tbc",  # The storage account name.
        # credential={},  # The credential used to authenticate the connection.
        # adls2_file_system="tbc",
        # adls2_prefix="tbc",
    },
}

current_resource = resources[EnvVar("DEV")]

cloud_assets = load_assets_from_current_module(group_name="cloud_assets")

cloud_assets_job = define_asset_job(
    name="cloud_assets",
    selection=AssetSelection.groups("cloud_assets"),
)


class CountryAssetConfig(DagsterConfig):
    asset_to_load: str
    partition_to_load: str | None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


# TODO. This is a temporary structure to list the end points of each country pipeline
# A more robust solution should be implemented as part of
# https://github.com/Urban-Analytics-Technology-Platform/popgetter/issues/38
# and https://github.com/Urban-Analytics-Technology-Platform/popgetter/issues/39
assets_to_monitor = (
    # UK Geography
    (AssetSelection.groups("uk") & AssetSelection.keys("boundary_line").downstream())
    # USA Geography
    | (AssetSelection.groups("us") & AssetSelection.keys("geometry_ids").downstream())
    # Belgium Geography + tables
    | (
        AssetSelection.groups("be")
        & AssetSelection.keys("be/source_table").downstream()
    )
)


@multi_asset_sensor(
    monitored_assets=assets_to_monitor,
    job=cloud_assets_job,
    minimum_interval_seconds=10,
)
def country_outputs_sensor(context):
    """
    Sensor that detects when the individual country outputs are available.
    """
    context.log.info("Country outputs are available")

    ic(context.latest_materialization_records_by_key())
    asset_events = context.latest_materialization_records_by_key()

    have_yielded_run_request = False
    for asset_key, execution_value in asset_events.items():
        if execution_value:
            # Try and get the partition key if it exists, otherwise default to None
            try:
                partitions_keys = (
                    execution_value.event_log_entry.dagster_event.event_specific_data.materialization.partition
                )
            except Exception:
                partitions_keys = None

            asset_to_load = str(asset_key.path[0])

            # Make a unique key for the run
            run_key = f"{asset_to_load}::{partitions_keys}"

            have_yielded_run_request = True
            yield RunRequest(
                run_key=run_key,
                run_config=RunConfig(
                    ops={
                        "load_cartography_gdf": CountryAssetConfig(
                            asset_to_load=asset_to_load,
                            partition_to_load=partitions_keys,
                        )
                    },
                ),
            )
            context.advance_cursor({asset_key: execution_value})

    if not have_yielded_run_request:
        # TODO: Probably don't need to provide this level of detail in the SkipReason
        materialized_asset_key_strs = [
            key.to_user_string() for key, value in asset_events.items() if value
        ]
        not_materialized_asset_key_strs = [
            key.to_user_string() for key, value in asset_events.items() if not value
        ]
        yield SkipReason(
            f"Observed materializations for {materialized_asset_key_strs}, "
            f"but not for {not_materialized_asset_key_strs}"
        )


@multi_asset(
    outs={
        "raw_cartography_gdf": AssetOut(),
        "source_asset_key": AssetOut(),
    },
    # TODO: This feels like a code smell. (mixing my metaphors)
    # It feels that this structure is duplicated and it ought
    # to be possible to have some reusable structure.
    config_schema={
        "asset_to_load": str,
        "partition_to_load": Noneable(str),
    },
)
def load_cartography_gdf(context):
    """
    Returns a GeoDataFrame of raw cartography data, from a country specific pipeline.
    """
    asset_to_load = context.run_config["ops"]["load_cartography_gdf"]["config"][
        "asset_to_load"
    ]

    # partition_to_load might not be present. Default to None if it is not present.
    partition_to_load = context.run_config["ops"]["load_cartography_gdf"]["config"].get(
        "partition_to_load", None
    )

    log_msg = f"Beginning conversion of cartography data from '{asset_to_load}' into cloud formats."
    context.log.info(log_msg)

    # TODO: This is horribly hacky - I do not like importing stuff in the middle of a module or function
    # However I cannot find another may to load the asset except
    # by using the Definitions object (or creating a RepositoryDefinition which seems to be deprecated)
    # Import the Definitions object here in the function, avoids a circular import error.
    from popgetter import defs as popgetter_defs

    cartography_gdf = popgetter_defs.load_asset_value(
        asset_to_load,
        partition_key=partition_to_load,
    )
    return (
        Output(cartography_gdf, output_name="raw_cartography_gdf"),
        Output(asset_to_load, output_name="source_asset_key"),
    )


@multi_asset(
    outs={
        "parquet_path": AssetOut(is_required=False),
        "flatgeobuff_path": AssetOut(is_required=False),
        "geojson_seq_path": AssetOut(is_required=False),
    },
    can_subset=True,
    required_resource_keys={"staging_res"},
)
def cartography_in_cloud_formats(context, raw_cartography_gdf, source_asset_key):
    """ "
    Returns dict of parquet, FlatGeobuf and GeoJSONSeq paths
    """
    staging_res = context.resources.staging_res
    # ic(staging_res)
    # ic(staging_res.staging_dir)
    staging_dir_str = staging_res.staging_dir

    staging_dir = Path(staging_dir_str)

    # Extract the selected keys from the context
    selected_keys = [
        str(key) for key in context.op_execution_context.selected_asset_keys
    ]

    if any("parquet_path" in key for key in selected_keys):
        ic("yielding parquet_path")
        parquet_path = staging_dir / f"{source_asset_key}.parquet"
        parquet_path.parent.mkdir(exist_ok=True)
        parquet_path.touch(exist_ok=True)
        raw_cartography_gdf.to_parquet(parquet_path)
        # upload_cartography_to_cloud(parquet_path)
        yield Output(value=parquet_path, output_name="parquet_path")

    if any("flatgeobuff_path" in key for key in selected_keys):
        ic("yielding flatgeobuff_path")
        flatgeobuff_path = staging_dir / f"{source_asset_key}.flatgeobuff"
        # Delete if the directory or file already exists:
        if flatgeobuff_path.exists():
            if flatgeobuff_path.is_dir():
                # Assuming that the directory is only one level deep
                for file in flatgeobuff_path.iterdir():
                    file.unlink()
                flatgeobuff_path.rmdir()
            else:
                flatgeobuff_path.unlink()
        raw_cartography_gdf.to_file(flatgeobuff_path, driver="FlatGeobuf")
        yield Output(value=flatgeobuff_path, output_name="flatgeobuff_path")

    if any("geojson_seq_path" in key for key in selected_keys):
        ic("yielding geojson_seq_path")
        geojson_seq_path = staging_dir / f"{source_asset_key}.geojsonseq"

        raw_cartography_gdf.to_file(geojson_seq_path, driver="GeoJSONSeq")
        yield Output(value=geojson_seq_path, output_name="geojson_seq_path")

    ic("end of cartography_in_cloud_formats")


def upload_cartography_to_cloud(context, cartography_in_cloud_formats):
    """
    Uploads the cartography files to the cloud.
    """
    log_msg = f"Uploading cartography to the cloud - {cartography_in_cloud_formats}"
    context.log.info(log_msg)


# Not working yet - need to figure out questions about how we run docker
# See comments here https://github.com/Urban-Analytics-Technology-Platform/popgetter/pull/68#issue-2205271531
# @asset()
# def generate_pmtiles(context, geojson_seq_path):
#     client = docker.from_env()
#     mount_folder = Path(geojson_seq_path).parent.resolve()

#     container = client.containers.run(
#         "stuartlynn/tippecanoe:latest",
#         "tippecanoe -o tracts.pmtiles tracts.geojsonseq",
#         volumes={mount_folder: {"bind": "/app", "mode": "rw"}},
#         detach=True,
#         remove=True,
#     )

#     output = container.attach(stdout=True, stream=True, logs=True)
#     for line in output:
#         context.log.info(line)
