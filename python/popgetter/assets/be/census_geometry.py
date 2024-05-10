from __future__ import annotations

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from dagster import (
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    AssetIn,
    asset,
)
from icecream import ic

from popgetter.utils import markdown_from_plot
from popgetter.metadata import GeometryMetadata, metadata_to_dataframe
from datetime import date

from .belgium import asset_prefix
from dataclasses import dataclass

geometry_metadata: GeometryMetadata = GeometryMetadata(
    validity_period_start=date(2023, 1, 1),
    validity_period_end=date(2023, 12, 31),
    level="municipality",
    # country -> province -> region -> arrondisement -> municipality
    hxl_tag="adm4",
)


@dataclass
class BelgiumGeometryLevel:
    level: str
    hxl_tag: str
    geo_id_column: str
    name_columns: dict[str, str]  # keys = language codes, values = column names


BELGIUM_GEOMETRY_LEVELS = {
    "province": BelgiumGeometryLevel(
        level="province",
        hxl_tag="adm1",
        geo_id_column="cd_prov_refnis",
        name_columns={
            "nld": "tx_prov_descr_nl",
            "fra": "tx_prov_descr_fr",
            "deu": "tx_prov_descr_de",
        },
    ),
    "region": BelgiumGeometryLevel(
        level="region",
        hxl_tag="adm2",
        geo_id_column="cd_rgn_refnis",
        name_columns={
            "nld": "tx_rgn_descr_nl",
            "fra": "tx_rgn_descr_fr",
            "deu": "tx_rgn_descr_de",
        },
    ),
    "arrondisement": BelgiumGeometryLevel(
        level="arrondisement",
        hxl_tag="adm3",
        geo_id_column="cd_dstr_refnis",
        name_columns={
            "nld": "tx_adm_dstr_descr_nl",
            "fra": "tx_adm_dstr_descr_fr",
            "deu": "tx_adm_dstr_descr_de",
        },
    ),
    "municipality": BelgiumGeometryLevel(
        level="municipality",
        hxl_tag="adm4",
        geo_id_column="cd_munty_refnis",
        name_columns={
            "nld": "tx_munty_descr_nl",
            "fra": "tx_munty_descr_fr",
            "deu": "tx_munty_descr_de",
        },
    ),
    "statistical_sector": BelgiumGeometryLevel(
        level="statistical_sector",
        hxl_tag="adm5",
        geo_id_column="cd_sector",
        name_columns={
            "nld": "tx_sector_descr_nl",
            "fra": "tx_sector_descr_fr",
            "deu": "tx_sector_descr_de",
        },
    ),
}


@asset(
    ins={
        "sector_geometries": AssetIn(
            key=[asset_prefix, "individual_census_table"],
            partition_mapping=SpecificPartitionsPartitionMapping(
                ["https://statbel.fgov.be/node/4726"]
            ),
        ),
    },
)
def geometry(
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
    level_details = BELGIUM_GEOMETRY_LEVELS["municipality"]

    geometry_metadata = GeometryMetadata(
        validity_period_start=date(2023, 1, 1),
        validity_period_end=date(2023, 12, 31),
        level=level_details.level,
        hxl_tag=level_details.hxl_tag,
    )

    region_geometries = (
        sector_geometries.dissolve(by=level_details.geo_id_column)
        .reset_index()
        .rename(columns={level_details.geo_id_column: "GEO_ID"})
        .loc[:, ["geometry", "GEO_ID"]]
    )
    ic(region_geometries.head())

    region_names = (
        sector_geometries.rename(
            columns={
                level_details.geo_id_column: "GEO_ID",
                level_details.name_columns["nld"]: "nld",
                level_details.name_columns["fra"]: "fra",
                level_details.name_columns["deu"]: "deu",
            }
        )
        .loc[:, ["GEO_ID", "nld", "fra", "deu"]]
        .drop_duplicates()
    )
    ic(region_names.head())

    # Generate a plot and convert the image to Markdown to preview it within
    # Dagster
    joined_gdf = region_geometries.merge(region_names, on="GEO_ID")
    ax = joined_gdf.plot(column="nld", legend=False)
    ax.set_title(f"Belgium 2023 {level_details.level}")
    md_plot = markdown_from_plot(plt)

    geometry_metadata_df = metadata_to_dataframe([geometry_metadata])

    context.add_output_metadata(
        metadata={
            "num_records": len(region_geometries),
            "geometry_plot": MetadataValue.md(md_plot),
            "names_preview": MetadataValue.md(region_names.head().to_markdown()),
            "metadata_preview": MetadataValue.md(
                geometry_metadata_df.head().to_markdown()
            ),
        },
    )

    return geometry_metadata_df, region_geometries, region_names
