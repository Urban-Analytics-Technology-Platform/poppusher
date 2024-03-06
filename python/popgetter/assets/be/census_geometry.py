from __future__ import annotations

import geopandas as gpd
import matplotlib.pyplot as plt
from dagster import (
    MetadataValue,
)
from icecream import ic

from popgetter.utils import markdown_from_plot

# TODO: Need to re-implement aggregate_sectors_to_municipalities to work with the sectors coming from the partitioned asset.


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
    ic(munty_gdf.head())
    ic(len(munty_gdf))

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
