from __future__ import annotations

from pathlib import Path

import docker
import geopandas as gpd
from dagster import (
    AssetOut,
    AssetSelection,
    EnvVar,
    Output,
    RunConfig,
    RunRequest,
    SkipReason,
    asset,
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


@multi_asset_sensor(
    monitored_assets=AssetSelection.keys("boundary_line").downstream(),
    job=cloud_assets_job,
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
            have_yielded_run_request = True
            yield RunRequest(
                run_key=context.cursor,
                run_config=RunConfig(
                    ops={
                        "raw_cartography_gdf": {
                            "config": {"asset_to_load": asset_key.path}
                        },
                    }
                ),
            )
        context.advance_all_cursors()

    if not have_yielded_run_request:
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


@asset()
def raw_cartography_gdf(context):
    """
    Returns a GeoDataFrame of raw cartography data, from a country specific pipeline.
    """
    asset_to_load = context.run_config["ops"]["raw_cartography_gdf"]["config"][
        "asset_to_load"
    ]
    log_msg = f"Beginning conversion of cartography data from '{asset_to_load}' into cloud formats."
    context.log.info(log_msg)

    # TODO: This is horribly hacky - I do not like importing stuff in the middle of a module or function
    # However I cannot find another may to load the asset except
    # by using the Definitions object (or creating a RepositoryDefinition which seems to be deprecated)
    # Import the Definitions object here in the function, avoids a circular import error.
    from popgetter import defs as popgetter_defs

    cartography_gdf = popgetter_defs.load_asset_value(asset_to_load)
    yield Output(cartography_gdf)


@multi_asset(
    outs={
        "parquet_path": AssetOut(is_required=False),
        "flatgeobuff_path": AssetOut(is_required=False),
        "geojson_seq_path": AssetOut(is_required=False),
    },
    can_subset=True,
)
def cartography_in_cloud_formats(context, raw_cartography_gdf: gpd.GeoDataFrame):
    """ "
    Returns dict of parquet, FlatGeobuf and GeoJSONSeq paths
    """
    parquet_path = "to_be_confirmed.parquet"
    flatgeobuff_path = "to_be_confirmed.flatgeobuff"
    geojson_seq_path = "to_be_confirmed.geojsonseq"

    if "parquet_path" in context.op_execution_context.selected_output_names:
        raw_cartography_gdf.to_parquet(parquet_path)
        # upload_cartography_to_cloud(parquet_path)
        yield Output(value=parquet_path, output_name="parquet_path")

    if "flatgeobuff_path" in context.op_execution_context.selected_output_names:
        raw_cartography_gdf.to_file(flatgeobuff_path, driver="FlatGeobuf")
        yield Output(value=flatgeobuff_path, output_name="flatgeobuff_path")

    if "geojson_seq_path" in context.op_execution_context.selected_output_names:
        raw_cartography_gdf.to_file(geojson_seq_path, driver="GeoJSONSeq")
        yield Output(value=geojson_seq_path, output_name="geojson_seq_path")


def upload_cartography_to_cloud(context, cartography_in_cloud_formats):
    """
    Uploads the cartography files to the cloud.
    """
    log_msg = f"Uploading cartography to the cloud - {cartography_in_cloud_formats}"
    context.log.info(log_msg)


@asset()
def generate_pmtiles(context, geojson_seq_path):
    client = docker.from_env()
    mount_folder = Path(geojson_seq_path).parent.resolve()

    container = client.containers.run(
        "stuartlynn/tippecanoe:latest",
        "tippecanoe -o tracts.pmtiles tracts.geojsonseq",
        volumes={mount_folder: {"bind": "/app", "mode": "rw"}},
        detach=True,
        remove=True,
    )

    output = container.attach(stdout=True, stream=True, logs=True)
    for line in output:
        context.log.info(line)
