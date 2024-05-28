from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from dagster import (
    AssetIn,
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    asset,
)
from icecream import ic

from popgetter.cloud_outputs import send_to_geometry_sensor, send_to_metadata_sensor
from popgetter.metadata import (
    GeometryMetadata,
    SourceDataRelease,
)
from popgetter.utils import markdown_from_plot

from .belgium import asset_prefix
from .census_tables import publisher


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


@send_to_geometry_sensor
@asset(
    ins={
        "sector_geometries": AssetIn(
            key=[asset_prefix, "individual_census_table"],
            partition_mapping=SpecificPartitionsPartitionMapping(
                ["https://statbel.fgov.be/node/4726"]
            ),
        ),
    },
    key_prefix=asset_prefix,
)
def geometry(
    context, sector_geometries
) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
    """
    Produces the full set of data / metadata associated with Belgian
    municipalities. The outputs, in order, are:

    1. A DataFrame containing a serialised GeometryMetadata object.
    2. A GeoDataFrame containing the geometries of the municipalities.
    3. A DataFrame containing the names of the municipalities (in this case,
       they are in Dutch, French, and German).
    """
    geometries_to_return = []

    for level_details in BELGIUM_GEOMETRY_LEVELS.values():
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

        geometries_to_return.append(
            (geometry_metadata, region_geometries, region_names)
        )

    # Add output metadata
    first_metadata, first_gdf, first_names = geometries_to_return[0]
    first_joined_gdf = first_gdf.merge(first_names, on="GEO_ID")
    ax = first_joined_gdf.plot(column="nld", legend=False)
    ax.set_title(f"Belgium 2023 {first_metadata.level}")
    md_plot = markdown_from_plot(plt)
    context.add_output_metadata(
        metadata={
            "all_geom_levels": MetadataValue.md(
                ",".join([metadata.level for metadata, _, _ in geometries_to_return])
            ),
            "first_geometry_plot": MetadataValue.md(md_plot),
            "first_names_preview": MetadataValue.md(first_names.head().to_markdown()),
        }
    )

    return geometries_to_return


@send_to_metadata_sensor
@asset(key_prefix=asset_prefix)
def source_data_releases(
    geometry: list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]
) -> dict[str, SourceDataRelease]:
    """
    Returns all SourceDataReleases for each geometry level.
    """
    return {
        geo_metadata.level: SourceDataRelease(
            name="StatBel Open Data",
            date_published=date(2015, 10, 22),
            reference_period_start=date(2015, 10, 22),
            reference_period_end=date(2015, 10, 22),
            collection_period_start=date(2015, 10, 22),
            collection_period_end=date(2015, 10, 22),
            expect_next_update=date(2022, 1, 1),
            url="https://statbel.fgov.be/en/open-data",
            description="TBC",
            data_publisher_id=publisher.id,
            geometry_metadata_id=geo_metadata.id,
        )
        for geo_metadata, _, _ in geometry
    }
