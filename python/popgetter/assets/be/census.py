from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from dagster import (
    MetadataValue,
    asset,
)

from popgetter.metadata import (
    DataPublisher,
    MetricMetadata,
    SourceDataRelease,
)
from popgetter.utils import download_zipped_files, markdown_from_plot

from .belgium import WORKING_DIR, asset_prefix, country

publisher: DataPublisher = DataPublisher(
    name="Statbel",
    url="https://statbel.fgov.be/en",
    description="Statbel is the Belgian statistical office. It is part of the Federal Public Service Economy, SMEs, Self-employed and Energy.",
    countries_of_interest=[country],
)

source: SourceDataRelease = SourceDataRelease(
    name="StatBel Open Data",
    date_published=date(2015, 10, 22),
    reference_period=(date(2015, 10, 22), None),
    collection_period=(date(2015, 10, 22), None),
    expect_next_update=date(2022, 1, 1),
    url="https://statbel.fgov.be/en/open-data",
    publishing_organisation=publisher,
    description="TBC",
    geography_file="TBC",
    geography_level="Municipality",
    # available_metrics=None,
    countries_of_interest=[country],
)
source.update_forward_refs()


metrics: dict[str, MetricMetadata] = {
    "pop_per_muni": MetricMetadata(
        human_readable_name="Population by place of residence, nationality, marital status, age and sex",
        source_metric_id="pop_per_muni",
        description="Population by place of residence, nationality, marital status, age and sex, per municipality",
        hxl_tag="x_tbc",
        metric_parquet_file_url=None,
        parquet_column_name="MS_POPULATION",
        parquet_margin_of_error_column=None,
        parquet_margin_of_error_file=None,
        potential_denominator_ids=None,
        parent_metric_id=None,
        source_data_release_id=source.id,
        source_download_url="https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2023.zip",
        source_archive_file_path="TF_SOC_POP_STRUCT_2023.txt",
        source_documentation_url="https://statbel.fgov.be/en/open-data/population-place-residence-nationality-marital-status-age-and-sex-13",
    ),
    "pop_per_sector": MetricMetadata(
        human_readable_name="Population per statistical sector",
        source_metric_id="pop_per_sector",
        description="Population per statistical sector",
        hxl_tag="x_tbc",
        metric_parquet_file_url=None,
        parquet_column_name="MS_POPULATION",
        parquet_margin_of_error_column=None,
        parquet_margin_of_error_file=None,
        potential_denominator_ids=None,
        parent_metric_id=None,
        source_data_release_id=source.id,
        source_download_url="https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip",
        source_archive_file_path="OPENDATA_SECTOREN_2022.txt",
        source_documentation_url="https://statbel.fgov.be/en/open-data/population-statistical-sector-10",
    ),
}


@asset(key_prefix=asset_prefix)
def get_publisher_metadata():
    """
    Returns a DataPublisher of metadata about the publisher.
    """
    return publisher


# @asset(key_prefix=asset_prefix)
# def get_census_metadata():
#     return source


@asset(key_prefix=asset_prefix)
def get_geometries(context) -> gpd.GeoDataFrame:
    # def get_geometries(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    """
    Downloads the Statistical Sector for Belgium and returns a GeoDataFrame.

    If the data has already been downloaded, it is not downloaded again and
    the cached version is used to create the GeoDataFrame.
    """
    output_dir = WORKING_DIR / "statistical_sectors" / "geometries"

    # Administrative boundaries - aka "Statistical sectors 2023 (Areas)"
    #
    # User WebUI:
    # "https://statbel.fgov.be/en/open-data/statistical-sectors-2023"

    # URL of datafile
    statistical_sectors = "https://statbel.fgov.be/sites/default/files/files/opendata/Statistische%20sectoren/sh_statbel_statistical_sectors_3812_20230101.geojson.zip"

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

    # zip_file_contents = "sh_statbel_statistical_sectors_3812_20230101.geojson"

    # # try:
    # url = f"zip://{zip_file_contents}::{statistical_sectors}"
    # local_path = get_path_to_cache(url, output_dir)

    # raise ValueError(
    #     f"{local_path}\n"
    #     f"{local_path.full_name}\n"
    #     f"{local_path.path}\n"
    #     f"{local_path.fs}\n"
    #     f"{local_path.fobjects}\n"
    #     f"{dir(local_path)}"
    # )

    # # except ValueError:
    # #     print("Skipping download of statistical sectors")

    try:
        download_zipped_files(statistical_sectors, str(output_dir))
    except ValueError:
        context.log.info(
            "File already stored locally. Skipping download of statistical sectors"
        )

    geojson_path = (
        output_dir
        / "sh_statbel_statistical_sectors_3812_20230101.geojson"
        / "sh_statbel_statistical_sectors_3812_20230101.geojson"
    )

    sectors_gdf = gpd.read_file(geojson_path)
    sectors_gdf.index = sectors_gdf.index.astype(str)

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    ax = sectors_gdf.plot(color="green")
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


@asset(key_prefix=asset_prefix)
def get_population_details_per_municipality(context) -> pd.DataFrame:
    metadata = metrics["pop_per_muni"]
    return get_census_table(context, metadata)


@asset(key_prefix=asset_prefix)
def get_population_by_statistical_sector(context):
    metadata = metrics["pop_per_muni"]
    return get_census_table(context, metadata)


def get_census_table(context, metadata: MetricMetadata) -> pd.DataFrame:
    with TemporaryDirectory() as temp_dir:
        extracted_file = download_zip(metadata, temp_dir)
        with Path(extracted_file).open() as f:
            population_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

    context.add_output_metadata(
        metadata={
            "num_records": len(population_df),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in population_df.columns.to_list()])
            ),
            "preview": MetadataValue.md(population_df.head().to_markdown()),
        }
    )

    return population_df


def download_zip(metadata: MetricMetadata, temp_dir) -> str:
    """
    Downloads a zip file from a URL and extracts it to a user-supplied temporary folder.

    It is expected that this will typically be used in a `with TemporaryDirectory:` statement, for example:

    ```
    with TemporaryDirectory() as temp_dir:
        extracted_file = download_zip(metadata, temp_dir)
        df = pd.read_csv(extracted_file)

    ```

    """

    if metadata.source_archive_file_path is None:
        err_msg = (
            f"Metadata for {metadata.source_metric_id} does not contain a path to the archive file"
            "Only use `download_zip` for files that are in an archive."
        )
        raise ValueError(err_msg)

    temp_dir = Path(temp_dir)
    temp_zip_file = temp_dir / "data.zip"

    with requests.get(metadata.source_download_url, stream=True) as r:
        r.raise_for_status()
        with Path(temp_zip_file).open(mode="wb") as f:
            for chunk in r.iter_content(chunk_size=(16 * 1024 * 1024)):
                f.write(chunk)

    with zipfile.ZipFile(temp_zip_file, "r") as z:
        return z.extract(metadata.source_archive_file_path, path=temp_dir)
