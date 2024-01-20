from __future__ import annotations

from tempfile import TemporaryDirectory

import geopandas as gpd
import matplotlib.pyplot as plt
from dagster import (
    AssetIn,
    MetadataValue,
    asset,
)

from popgetter.utils import markdown_from_plot

from .belgium import asset_prefix
from .census_tables import download_file


@asset(key_prefix=asset_prefix)
def get_sector_geometries(context) -> gpd.GeoDataFrame:
    # def get_geometries(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    """
    Downloads the Statistical Sector for Belgium and returns a GeoDataFrame.

    """

    # Administrative boundaries - aka "Statistical sectors 2023 (Areas)"
    #
    # User WebUI:
    # "https://statbel.fgov.be/en/open-data/statistical-sectors-2023"

    # Column Descriptions from https://statbel.fgov.be/sites/default/files/files/opendata/Statistische%20sectoren/Columns%20description_0.xlsx

    # | Naam/Nom             | Description                                                                           |
    # | -------------------- | ------------------------------------------------------------------------------------- |
    # | cd_sector            | Statistical sector code on 01/01/2018                                                 |
    # | tx_sector_descr_nl   | Name of the statistical sector, Dutch version                                         |
    # | tx_sector_descr_fr   | Name of the statistical sector, French version                                        |
    # | tx_sector_descr_de   | Name of the statistical sector, German version                                        |
    # | cd_sub_munty         | Aggregation of statistical sectors that have the same first 6 positions in their code |
    # | tx_sub_munty_nl      | NIS6 name, Dutch version                                                              |
    # | tx_sub_munty_fr      | NIS6 name, French version                                                             |
    # | cd_munty_refnis      | NIS code of the municipality on 01/01/2018 as text                                    |
    # | tx_munty_descr_nl    | Name of the municipality, Dutch version                                               |
    # | tx_munty_descr_fr    | Name of the municipality, French version                                              |
    # | tx_munty_descr_de    | Name of the municipality, German version                                              |
    # | cd_dstr_refnis       | NIS Code of the district                                                              |
    # | tx_adm_dstr_descr_nl | Name of the district, Dutch version                                                   |
    # | tx_adm_dstr_descr_fr | Name of the district, French version                                                  |
    # | tx_adm_dstr_descr_de | Name of the district, German version                                                  |
    # | cd_prov_refnis       | NIS Code of the province                                                              |
    # | tx_prov_descr_nl     | Name of the province, Dutch version                                                   |
    # | tx_prov_descr_fr     | Name of the province, French version                                                  |
    # | tx_prov_descr_de     | Name of the province, German version                                                  |
    # | cd_rgn_refnis        | NIS Code of the region                                                                |
    # | tx_rgn_descr_nl      | Name of the region, Dutch version                                                     |
    # | tx_rgn_descr_fr      | Name of the region, French version                                                    |
    # | tx_rgn_descr_de      | Name of the region, German version                                                    |
    # | cd_country           | Code of the country                                                                   |
    # | cd_nuts1             | NUTS1 code, version 2016 used by Eurostat                                             |
    # | cd_nuts2             | NUTS2 code, version 2016 used by Eurostat                                             |
    # | cd_nuts3             | NUTS3 code, version 2016 used by Eurostat                                             |
    # | ms_area_ha           | Area of the statistical sector in hectares calculated in RS Lambert 2008)             |
    # | ms_perimeter_m       | Perimeter of the statistical sector in meters calculated in RS Lambert 2008)          |

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


@asset(
    key_prefix=asset_prefix,
    ins={
        "get_sector_geometries": AssetIn(key_prefix=asset_prefix),
    },
)
def aggregate_sectors_to_municipalities(
    context, get_sector_geometries
) -> gpd.GeoDataFrame:
    """
    Aggregates a GeoDataFrame of the Statistical Sectors to Municipalities.

    The `sectors_gdf` is assumed to be produced by `get_geometries()`.

    Also saves the result to a GeoPackage file in the output_dir
    returns a GeoDataFrame of the Municipalities.
    """
    # output_dir = WORKING_DIR / "statistical_sectors"
    munty_gdf = get_sector_geometries.dissolve(by="cd_munty_refnis")
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
