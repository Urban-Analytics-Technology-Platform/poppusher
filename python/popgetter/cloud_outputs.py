from __future__ import annotations

from pathlib import Path

import docker
import geopandas as gpd
from dagster import AssetOut, Output, asset, multi_asset


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
        yield Output(parquet_path)

    if "flatgeobuff_path" in context.op_execution_context.selected_output_names:
        raw_cartography_gdf.to_file(flatgeobuff_path, driver="FlatGeobuf")
        yield Output(flatgeobuff_path)

    if "geojson_seq_path" in context.op_execution_context.selected_output_names:
        raw_cartography_gdf.to_file(geojson_seq_path, driver="GeoJSONSeq")
        yield Output(geojson_seq_path)


# @op(io_manager_key="s3")
def upload_cartography_to_cloud(context, cartography_in_cloud_formats):
    """
    Uploads the cartography files to the cloud.
    """
    log_msg = f"Uploading cartography to the cloud - {cartography_in_cloud_formats}"
    context.log.info(log_msg)


@asset()
def generate_pmtiles(context, cartography_in_cloud_formats):
    client = docker.from_env()
    mount_folder = Path(
        cartography_in_cloud_formats["geojson_seq_path"]
    ).parent.resolve()

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
