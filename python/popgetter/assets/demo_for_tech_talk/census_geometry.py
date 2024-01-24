from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from dagster import (
    MetadataValue,
    asset,
)

from popgetter.utils import markdown_from_plot

demo_data_dir = (
    Path(__file__).parent.parent.parent.parent.parent / "tests" / "demo_data"
)


@asset()
def simplest_sector_geometries() -> gpd.GeoDataFrame:
    """
    Downloads the Statistical Sector for Belgium and returns a GeoDataFrame.
    """
    # DEMO: pretend we are downloading a file:
    input_path = str(demo_data_dir / "be_demo_sector.geojson")
    # read the file
    return gpd.read_file(input_path)


@asset()
def sector_geometries(context) -> gpd.GeoDataFrame:
    """
    Downloads the Statistical Sector for Belgium and returns a GeoDataFrame.
    This time with some metadata.
    """
    # DEMO: pretend we are downloading a file:
    input_path = str(demo_data_dir / "be_demo_sector.geojson")
    sectors_gdf = gpd.read_file(input_path)
    # DEMO: end of pretend code

    # Set the type of the index
    sectors_gdf.index = sectors_gdf.index.astype(str)

    # Plot and convert the image to Markdown to preview it within Dagster
    ax = sectors_gdf.plot(column="tx_sector_descr_nl", legend=False)
    ax.set_title("Sectors in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        # Metadata can be any key-value pair
        metadata={
            "num_records": len(sectors_gdf),
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


@asset()
def aggregate_sectors_to_municipalities(context, sector_geometries) -> gpd.GeoDataFrame:
    """
    Aggregates a GeoDataFrame of the Statistical Sectors to Municipalities.

    The `sector_geometries` is assumed to be produced by `sector_geometries()`.

    Also saves the result to a GeoPackage file in the output_dir
    returns a GeoDataFrame of the Municipalities.
    """
    muni_gdf = sector_geometries.dissolve(by="cd_munty_refnis")
    muni_gdf.index = muni_gdf.index.astype(str)

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    # ax = munty_gdf.plot(color="green")
    ax = muni_gdf.plot(column="tx_sector_descr_nl", legend=False)
    ax.set_title("Municipalities in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(muni_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in muni_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                muni_gdf.loc[:, muni_gdf.columns != "geometry"].head().to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return muni_gdf
