from __future__ import annotations

import geopandas as gpd
import matplotlib.pyplot as plt
from dagster import (
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    multi_asset,
    AssetIn,
    AssetOut,
)
import pandas as pd
from icecream import ic

from popgetter.utils import markdown_from_plot

from .belgium import asset_prefix


@multi_asset(
    ins={
        "sector_geometries": AssetIn(
            key=[asset_prefix, "individual_census_table"],
            partition_mapping=SpecificPartitionsPartitionMapping(
                ["https://statbel.fgov.be/node/4726"]
            ),
        ),
    },
    outs={
        "municipality_geometries": AssetOut(key_prefix=asset_prefix),
        "municipality_names": AssetOut(key_prefix=asset_prefix),
    },
)
def aggregate_sectors_to_municipalities(
    context, sector_geometries
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Produces a GeoDataFrame containing the municipalities of Belgium, plus
    a DataFrame with their names in Dutch, French and German.
    """
    municipality_geometries = (
        sector_geometries.dissolve(by="cd_munty_refnis")
        .reset_index()
        .rename(columns={"cd_munty_refnis": "GEO_ID"})
        .loc[:, ["geometry", "GEO_ID"]]
    )
    ic(municipality_geometries.head())

    municipality_names = (
        sector_geometries.rename(
            columns={
                "cd_munty_refnis": "GEO_ID",
                "tx_munty_descr_nl": "nld",
                "tx_munty_descr_fr": "fra",
                "tx_munty_descr_de": "deu",
            }
        )
        .loc[:, ["GEO_ID", "nld", "fra", "deu"]]
        .drop_duplicates()
    )
    ic(municipality_names.head())

    # Generate a plot and convert the image to Markdown to preview it within
    # Dagster
    joined_gdf = municipality_geometries.merge(municipality_names, on="GEO_ID")
    ax = joined_gdf.plot(column="nld", legend=False)
    ax.set_title("Municipalities in Belgium")
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        output_name="municipality_geometries",
        metadata={
            "num_records": len(municipality_geometries),
            "name_columns": MetadataValue.md(
                "\n".join(
                    [f"- '`{col}`'" for col in municipality_names.columns.to_list()]
                )
            ),
            "preview": MetadataValue.md(municipality_names.head().to_markdown()),
            "plot": MetadataValue.md(md_plot),
        },
    )

    return municipality_geometries, municipality_names
