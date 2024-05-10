from __future__ import annotations

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from dagster import (
    AssetIn,
    AssetOut,
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    multi_asset,
)
from icecream import ic

from popgetter.utils import markdown_from_plot
from popgetter.metadata import GeometryMetadata, metadata_to_dataframe
from datetime import date

from .belgium import asset_prefix

geometry_metadata: GeometryMetadata = GeometryMetadata(
    validity_period_start=date(2023, 1, 1),
    validity_period_end=date(2023, 12, 31),
    level="municipality",
    # country -> province -> region -> arrondisement -> municipality
    hxl_tag="adm4",
)


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
        "geometry_metadata": AssetOut(key_prefix=asset_prefix),
        "geometry": AssetOut(key_prefix=asset_prefix),
        "geometry_names": AssetOut(key_prefix=asset_prefix),
    },
)
def municipalities(
    context, sector_geometries
) -> tuple[pd.DataFrame, gpd.GeoDataFrame, pd.DataFrame]:
    """
    Produces the full set of data / metadata associated with Belgian
    municipalities. The outputs, in order, are:
    
    1. A DataFrame containing a serialised GeometryMetadata object.
    2. A GeoDataFrame containing the geometries of the municipalities.
    3. A DataFrame containing the names of the municipalities (in this case,
       they are in Dutch, French, and German).
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

    geometry_metadata_df = metadata_to_dataframe([geometry_metadata])

    context.add_output_metadata(
        output_name="geometry_metadata",
        metadata={
            "preview": MetadataValue.md(geometry_metadata_df.head().to_markdown()),
        },
    )
    context.add_output_metadata(
        output_name="geometry",
        metadata={
            "num_records": len(municipality_geometries),
            "plot": MetadataValue.md(md_plot),
        },
    )
    context.add_output_metadata(
        output_name="geometry_names",
        metadata={
            "num_records": len(municipality_names),
            "name_columns": MetadataValue.md(
                "\n".join(
                    [f"- '`{col}`'" for col in municipality_names.columns.to_list()]
                )
            ),
            "preview": MetadataValue.md(municipality_names.head().to_markdown()),
        },
    )

    return geometry_metadata_df, municipality_geometries, municipality_names
