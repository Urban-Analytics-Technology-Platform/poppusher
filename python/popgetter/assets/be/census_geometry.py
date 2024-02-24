from __future__ import annotations

from tempfile import TemporaryDirectory

import geopandas as gpd
import matplotlib.pyplot as plt
from dagster import (
    MetadataValue,
)

from popgetter.utils import markdown_from_plot

from .census_tables import download_file


# @asset(key_prefix=asset_prefix)
def sector_geometries(context) -> gpd.GeoDataFrame:
    # def get_geometries(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    """
    Downloads the Statistical Sector for Belgium and returns a GeoDataFrame.

    """
    # URL of datafile
    statistical_sectors = "https://statbel.fgov.be/sites/default/files/files/opendata/Statistische%20sectoren/sh_statbel_statistical_sectors_3812_20230101.geojson.zip"
    internal_file_path = "sh_statbel_statistical_sectors_3812_20230101.geojson/sh_statbel_statistical_sectors_3812_20230101.geojson"

    with TemporaryDirectory() as temp_dir:
        geojson_path = download_file(statistical_sectors, internal_file_path, temp_dir)
        sectors_gdf = gpd.read_file(geojson_path)

    # Set the type of the index
    sectors_gdf.index = sectors_gdf.index.astype(str)

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    # ax = sectors_gdf.plot(color="green")
    ax = sectors_gdf.plot(column="tx_sector_descr_nl", legend=False)
    ax.set_title("Sectors in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(sectors_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in sectors_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                sectors_gdf.loc[:, sectors_gdf.columns != "geometry"]
                .head()
                .to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return sectors_gdf


# @asset(
#     key_prefix=asset_prefix,
#     ins={
#         "sector_geometries": AssetIn(key_prefix=asset_prefix),
#     },
# )
def aggregate_sectors_to_municipalities(context, sector_geometries) -> gpd.GeoDataFrame:
    """
    Aggregates a GeoDataFrame of the Statistical Sectors to Municipalities.

    The `sectors_gdf` is assumed to be produced by `get_geometries()`.

    Also saves the result to a GeoPackage file in the output_dir
    returns a GeoDataFrame of the Municipalities.
    """
    # output_dir = WORKING_DIR / "statistical_sectors"
    munty_gdf = sector_geometries.dissolve(by="cd_munty_refnis")
    # munty_gdf.to_file(output_dir / "municipalities.gpkg", driver="GPKG")
    munty_gdf.index = munty_gdf.index.astype(str)

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    # ax = munty_gdf.plot(color="green")
    ax = munty_gdf.plot(column="tx_sector_descr_nl", legend=False)
    ax.set_title("Municipalities in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(munty_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in munty_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                munty_gdf.loc[:, munty_gdf.columns != "geometry"].head().to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return munty_gdf
