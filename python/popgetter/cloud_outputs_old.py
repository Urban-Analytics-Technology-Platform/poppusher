from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

import geopandas as gpd
from dagster import (
    AssetKey,
    AssetSelection,
    Config,
    DefaultSensorStatus,
    DynamicPartitionsDefinition,
    Noneable,
    Output,
    RunConfig,
    RunRequest,
    SkipReason,
    asset,
    define_asset_job,
    load_assets_from_current_module,
    multi_asset_sensor,
)
from icecream import ic
from slugify import slugify

cloud_assets = load_assets_from_current_module(group_name="cloud_assets")


publishing_partition = DynamicPartitionsDefinition(name="publishing_partition")


cloud_assets_job = define_asset_job(
    name="cloud_assets",
    selection=AssetSelection.groups("cloud_assets"),
    partitions_def=publishing_partition,
)


class CountryAssetConfig(Config):
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
    (
        AssetSelection.groups("uk")
        & AssetSelection.keys("boundary_line").downstream(include_self=False)
    )
    # USA Geography
    | (
        AssetSelection.groups("us")
        & AssetSelection.keys("geometry_ids").downstream(include_self=False)
    )
    # Belgium Geography + tables
    | (
        AssetSelection.groups("be")
        # Note: include_self=True as the geodataframe is currently not output for
        # derived tables but only for source table:
        # be-source-table-https-statbel-fgov-be-node-4726
        & AssetSelection.keys("be/source_table").downstream(include_self=True)
    )
)


@multi_asset_sensor(
    monitored_assets=assets_to_monitor,
    job=cloud_assets_job,
    minimum_interval_seconds=10,
    default_status=DefaultSensorStatus.STOPPED,
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
        context.log.debug(
            ic(f"asset_key: {asset_key}, execution_value: {execution_value}")
        )
        if execution_value:
            asset_to_load = asset_key.to_user_string()

            # Try and get the partition key if it exists, otherwise default to None
            try:
                source_partitions_keys = (
                    execution_value.event_log_entry.dagster_event.event_specific_data.materialization.partition
                )
            except Exception:
                source_partitions_keys = None

            context.log.debug(ic(f"source_partitions_keys: {source_partitions_keys}"))

            # Make a unique key for the run
            if source_partitions_keys:
                run_key = slugify(f"{asset_to_load}-{source_partitions_keys}")
            else:
                run_key = slugify(asset_to_load)

            context.log.debug(ic(f"run_key: {run_key}"))

            # Required, during development, when the pattern for the `run_key`` is changed
            if False:
                for partition_key in context.instance.get_dynamic_partitions(
                    "publishing_partition"
                ):
                    context.instance.delete_dynamic_partition(
                        "publishing_partition", partition_key
                    )

            # Add the run_key to the dynamic partitions if it is not already there
            if run_key not in context.instance.get_dynamic_partitions(
                "publishing_partition"
            ):
                context.instance.add_dynamic_partitions(
                    partitions_def_name="publishing_partition", partition_keys=[run_key]
                )

            for partition_key in context.instance.get_dynamic_partitions(
                "publishing_partition"
            ):
                context.log.debug(ic(f"available partition_key: {partition_key}"))

            have_yielded_run_request = True
            yield RunRequest(
                # TODO: run_key is set to None for now, as it is convenient to enable re-running the sensor
                # whilst debugging. A proper run_key should be included as part of
                # # https://github.com/Urban-Analytics-Technology-Platform/popgetter/issues/38
                # and https://github.com/Urban-Analytics-Technology-Platform/popgetter/issues/39
                run_key=None,
                # run_key=run_key,
                run_config=RunConfig(
                    ops={
                        "upstream_df": CountryAssetConfig(
                            asset_to_load=asset_to_load,
                            partition_to_load=source_partitions_keys,
                        )
                    },
                ),
                partition_key=run_key,
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


@asset(
    # todo: this feels like a code smell. (mixing my metaphors)
    # it feels that this structure is duplicated and it ought
    # to be possible to have some reusable structure.
    config_schema={
        "asset_to_load": str,
        "partition_to_load": Noneable(str),
    },
    partitions_def=publishing_partition,
)
def upstream_df(context):
    """
    Returns a GeoDataFrame of raw cartography data, from a country specific pipeline.
    """
    asset_to_load = context.run_config["ops"]["upstream_df"]["config"]["asset_to_load"]

    # partition_to_load might not be present. Default to None if it is not present.
    partition_to_load = context.run_config["ops"]["upstream_df"]["config"].get(
        "partition_to_load", None
    )

    log_msg = f"Beginning conversion of cartography data from '{asset_to_load}' (partition_key={partition_to_load}) into cloud formats."
    context.log.info(log_msg)

    # TODO: This is horribly hacky - I do not like importing stuff in the middle of a module or function
    # However I cannot find another may to load the asset except
    # by using the Definitions object (or creating a RepositoryDefinition which seems to be deprecated)
    # Import the Definitions object here in the function, avoids a circular import error.
    from popgetter import defs as popgetter_defs

    cartography_gdf = popgetter_defs.load_asset_value(
        AssetKey(asset_to_load.split("/")),
        partition_key=partition_to_load,
    )

    if isinstance(cartography_gdf, gpd.GeoDataFrame):
        return Output(cartography_gdf)

    # TODO: This is a temporary solution. We need to handle non-GeoDataFrame types
    err_msg = f"Expected a GeoDataFrame, but got {type(cartography_gdf)}. Handling non-GeoDataFrame types is not yet implemented."
    context.log.error(ic(err_msg))
    raise ValueError(err_msg)


def df_to_bytes(df: gpd.GeoDataFrame, output_type: str) -> bytes:
    tmp = tempfile.NamedTemporaryFile(prefix=str(uuid.uuid4()))
    if output_type.lower() == "parquet":
        df.to_parquet(tmp.name + ".parquet")
    elif output_type.lower() == "flatgeobuf":
        df.to_file(tmp.name + ".flatgeobuf", driver="FlatGeobuf")
    elif output_type.lower() == "geojsonseq":
        df.to_file(tmp.name + ".geojsonseq", driver="GeoJSONSeq")
    else:
        value_error: str = f"'{output_type}' is not currently supported."
        raise ValueError(value_error)
    with Path(tmp.name).open(mode="rb") as f:
        return f.read()


@asset(io_manager_key="publishing_io_manager", partitions_def=publishing_partition)
def parquet(context, upstream_df):  # noqa: ARG001
    return df_to_bytes(upstream_df, "parquet")


@asset(io_manager_key="publishing_io_manager", partitions_def=publishing_partition)
def flatgeobuf(context, upstream_df):  # noqa: ARG001
    return df_to_bytes(upstream_df, "flatgeobuf")


@asset(io_manager_key="publishing_io_manager", partitions_def=publishing_partition)
def geojsonseq(context, upstream_df):  # noqa: ARG001
    return df_to_bytes(upstream_df, "geojsonseq")


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
