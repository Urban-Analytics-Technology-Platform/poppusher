from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urljoin
from typing import ClassVar
import os

import pandas as pd
from pandas import DataFrame
import requests
from bs4 import BeautifulSoup
from dagster import DynamicPartitionsDefinition, MetadataValue, asset
from icecream import ic

from popgetter.metadata import CountryMetadata, DataPublisher, GeometryMetadata, MetricMetadata, SourceDataRelease
from popgetter.utils import SourceDataAssumptionsOutdated, add_metadata, extract_main_file_from_zip
from popgetter.assets.country import Country

# TODO:
# - Create a asset which is a catalog of the available data / tables / metrics
# - This catalog must include a field which is the smallest geometry level where the data is available
# - The geometry level is only discoverable after downloading the zip file
# - The zip files can contain multiple CSV files, one for each geometry level
# - Some of the downloaded files mistakenly have two consecutive `.` in the filename, e.g. `census2021-ts002-lsoa..csv`. We need to be able to gracefully handle this
# - The catalog must to parsed into an Dagster Partition, so that
#    - individual tables can be uploaded to the cloud table sensor
#    - the metadata object can be created for each table/metric
from .united_kingdom import country, asset_prefix



EW_GEO_LEVELS = [
    "oa",
    "lsoa",
    "msoa",
    "ltla",
    "rgn",
    "ctry",
]

# TODO - this is probably only required for tests, 
# hence would be best move to a test fixture
REQUIRED_TABLES = ["TS009"] if os.getenv("ENV") == "dev" else None


class EnglandAndWales(Country):
    key_prefix: ClassVar[str]  = "england_wales"
    geo_levels: ClassVar[list[str]] = EW_GEO_LEVELS
    required_tables: list[str] | None = REQUIRED_TABLES

    def _country_metadata(self, context) -> CountryMetadata:
        return country
    
    def _data_publisher(self, context, country_metdata: CountryMetadata) -> DataPublisher:
        # TODO - add proper details here
        return DataPublisher(
            name="ONS - fix me!",
            url="https://www.nomisweb.co.uk/sources/census_2021_bulk",
            description="ONS - fix me!",
            countries_of_interest=[country.id],
        )
    
    def _catalog(self, context) -> pd.DataFrame:
        self.remove_all_partition_keys(context)

        catalog_summary = {
            "node": [],
            "partition_key": [],
            "table_id": [],
            "geo_level": [],
            "human_readable_name": [],
            "description": [],
            "metric_parquet_file_url": [],
            "parquet_column_name": [],
            "parquet_margin_of_error_column": [],
            "parquet_margin_of_error_file": [],
            "potential_denominator_ids": [],
            "parent_metric_id": [],
            "source_data_release_id": [],
            "source_download_url": [],
            "source_format": [],
            "source_archive_file_path": [],
            "source_documentation_url": [],
        }

        bulk_downloads_df = bulk_downloads_webpage(context)

        for bulk_downloads_index, row in bulk_downloads_df.iterrows():

            columns = [
                "table_id",
                "table_name",
                "original_release_filename",
                "original_release_url",
                "extra_post_release_filename",
                "extra_post_release_url",
            ]

            table_id = row["table_id"]

            source_documentation_url = _guess_source_documentation_url(table_id)

            # Get description of the table
            # TODO - For now this is page scraping the description from the source_documentation_url page
            # In the future we should retrieve the description by finding a suitable API call.
            description = row["table_name"]
            description = _retrieve_table_description(source_documentation_url)
            _api_url = "https://www.nomisweb.co.uk/api/v01/dataset/nm_1_1.overview.json?select=DateMetadata,DatasetMetadata,Dimensions,DimensionMetadata"


            # For now this does not use the "extra_post_release_filename" and "extra_post_release_url" tables
            for geo_level in self.geo_levels:
                # get the path within the zip file
                archive_file_path = _guess_csv_filename(row["original_release_filename"], geo_level)

                catalog_summary["node"].append(bulk_downloads_index)
                catalog_summary["table_id"].append(table_id)
                catalog_summary["geo_level"].append(geo_level)
                catalog_summary["partition_key"].append(f"{geo_level}/{table_id}")
                catalog_summary["human_readable_name"].append(row["table_name"])
                # TODO - For now this is the same as the human readable name
                # In the future we should retrieve the description by scraping the page or finding a suitable API call.
                catalog_summary["description"].append(description)
                catalog_summary["metric_parquet_file_url"].append(None)
                catalog_summary["parquet_column_name"].append(None)
                catalog_summary["parquet_margin_of_error_column"].append(None)
                catalog_summary["parquet_margin_of_error_file"].append(None)
                catalog_summary["potential_denominator_ids"].append(None)
                catalog_summary["parent_metric_id"].append(None)
                catalog_summary["source_data_release_id"].append(None)
                catalog_summary["source_download_url"].append(row["original_release_url"])
                catalog_summary["source_format"].append(None)
                catalog_summary["source_archive_file_path"].append(archive_file_path)
                catalog_summary["source_documentation_url"].append(source_documentation_url)

        catalog_df = pd.DataFrame.from_records(catalog_summary)
        self.add_partition_keys(context, catalog_df["partition_key"].to_list())

        add_metadata(context, catalog_df, "Catalog")
        return catalog_df

    def _census_tables(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        pass

    def _derived_metrics(self, context, census_tables: DataFrame, source_metric_metadata: MetricMetadata) -> tuple[list[MetricMetadata], DataFrame]:
        pass

    def _metrics(self, context, derived_metrics: list[MetricMetadata]) -> pd.DataFrame:
        pass

    def _source_data_releases(self, context, geometry: list[tuple[GeometryMetadata, Any, DataFrame]], data_publisher: DataPublisher) -> dict[str, SourceDataRelease]:
        pass

    def _source_metric_metadata(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        pass

    def _geometry(self, context) -> list[tuple[GeometryMetadata, Any, DataFrame]]:
        pass
    


def _guess_source_documentation_url(table_id):
    return f"https://www.nomisweb.co.uk/datasets/c2021{table_id.lower()}"

def _retrieve_table_description(source_documentation_url):
    soup = BeautifulSoup(requests.get(source_documentation_url).content, features="lxml")
    landing_info = soup.find_all(id="dataset-landing-information")

    try:
        assert len(landing_info) == 1
        landing_info = landing_info[0]
    except AssertionError as ae:
        raise SourceDataAssumptionsOutdated(ae, f"Expected a single section with `id=dataset-landing-information`, but found {len(landing_info)}.")

    description = "\n".join([text.strip() for text in landing_info.stripped_strings])
    return description


def _guess_csv_filename(zip_filename, geometry_level):
    """
    Guess the name of the main file in the zip file.
    """
    stem = Path(zip_filename).stem
    return f"{stem}-{geometry_level}.csv"



# bulk_tables_partition = DynamicPartitionsDefinition(name="bulk_tables")


# @asset(
#     partitions_def=bulk_tables_partition,
#     key_prefix=asset_prefix,
#     description="Table of available bulk downloads from the Census 2021 website.",
# )
def bulk_downloads_webpage(context) -> pd.DataFrame:
    """
    Get the list of bulk zip files from the bulk downloads page.
    """
    bulk_downloads_page = "https://www.nomisweb.co.uk/sources/census_2021_bulk"
    columns = ["table_id", "description", "original_release", "extra_post_release"]
    dfs = pd.read_html(bulk_downloads_page, header=0, extract_links="all")

    if len(dfs) != 1:
        raise SourceDataAssumptionsOutdated(
            f"Expected a single table on the bulk downloads page, but found {len(dfs)} tables."
        )

    # The first table is the one we want
    download_df = dfs[0]
    download_df.columns = columns

    # There are some subheadings in the table, which are added as rows by `read_html`
    # These can be identified by the `table_id` == `description` == `original_release_filename`
    # We need to drop these rows
    download_df = download_df[download_df["table_id"] != download_df["description"]]
    # expand the tuples into individual columns
    return _expand_tuples_in_df(download_df)


def _expand_tuples_in_df(df) -> pd.DataFrame:
    """
    Expand the tuples in the DataFrame.
    """
    root_url = "https://www.nomisweb.co.uk/"

    columns = [
        "table_id",
        "table_name",
        "original_release_filename",
        "original_release_url",
        "extra_post_release_filename",
        "extra_post_release_url",
    ]
    new_df = pd.DataFrame(columns=columns)

    # Copy individual columns from the tuples
    # If there is a URL, it is in the second element of the tuple, and should be joined with the root URL
    # "table_id" and "description" do not have URLs
    new_df["table_id"] = df["table_id"].apply(lambda x: x[0])
    new_df["table_name"] = df["description"].apply(lambda x: x[0])
    new_df["original_release_filename"] = df["original_release"].apply(lambda x: x[0])
    new_df["original_release_url"] = df["original_release"].apply(
        lambda x: urljoin(root_url, x[1])
    )

    # There may not be a valid value for "extra_post_release", hence the check using `isinstance`
    new_df["extra_post_release_filename"] = df["extra_post_release"].apply(
        lambda x: x[0] if isinstance(x, tuple) else None
    )
    new_df["extra_post_release_url"] = df["extra_post_release"].apply(
        lambda x: urljoin(root_url, x[1]) if isinstance(x, tuple) else None
    )

    return new_df


# @asset(partitions_def=bulk_tables_partition, key_prefix=asset_prefix)
def bulk_tables_df(context, bulk_downloads_webpage):
    """
    WIP:
    For now this function:
    - downloads all of the "main" zip files
    - extracts all of the CSV files from each zip file
    - reads the CSV files into a DataFrame
    - lists the columns of the DataFrame
    """
    table_id = context.partition_key
    ic(table_id)

    current_table = bulk_downloads_webpage[
        bulk_downloads_webpage["table_id"] == table_id
    ]
    ic(current_table)

    description = current_table["description"].values[0]
    original_release_url = current_table["original_release_url"].values[0]
    original_release_filename = current_table["original_release_filename"].values[0]

    all_columns = []

    with TemporaryDirectory() as temp_dir:
        ic(original_release_url)
        temp_zip = Path(_download_zipfile(original_release_url, temp_dir))

        for geom, csv_filebase in _guess_csv_filename(original_release_filename):
            ic(geom, csv_filebase)

            # This is a workaround for the fact that some of the filenames have two
            # consecutive `.` in the filename
            for ext in [".csv", "..csv"]:
                extract_file_path = None
                csv_filename = f"{csv_filebase}{ext}"
                try:
                    extract_file_path = extract_main_file_from_zip(
                        temp_zip, Path(temp_dir), csv_filename
                    )
                    break
                except ValueError:
                    pass

            if extract_file_path:
                ic(extract_file_path)
                df = pd.read_csv(extract_file_path)

                ic(df.columns.to_list)

                for col in df.columns:
                    all_columns.append((table_id, description, geom, col))

    columns_df = pd.DataFrame(
        all_columns, columns=["table_id", "description", "geom", "column_name"]
    )

    # Add some metadata to the context
    metadata = {
        "title": "Table of available bulk downloads from the Census 2021 website.",
        "num_records": len(columns_df),  # Metadata can be any key-value pair
        "columns": MetadataValue.md(
            "\n".join([f"- '`{col}`'" for col in columns_df.columns.to_list()])
        ),
        "preview": MetadataValue.md(columns_df.to_markdown()),
    }

    context.add_output_metadata(metadata=metadata)

    return columns_df


def create_metric_metadata(table_id, geom, column_name):
    # mmd = MetricMetadata(
    #     human_readable_name=column_name,
    #     source_download_url=,
    #     source_archive_file_path=,
    #     source_documentation_url=,
    #     source_data_release_id=source_data_release.id,
    #     parent_metric_id=None,
    #     potential_denominator_ids=None,
    #     parquet_margin_of_error_file=None,
    #     parquet_margin_of_error_column=None,
    #     parquet_column_name=,
    #     metric_parquet_path="__PLACEHOLDER__",
    #     hxl_tag=None,
    #     description=column_name,
    #     source_metric_id=column_name,
    # )
    pass



def _download_zipfile(source_download_url, temp_dir) -> str:
    temp_dir = Path(temp_dir)
    temp_file = temp_dir / "data.zip"

    with requests.get(source_download_url, stream=True) as r:
        r.raise_for_status()
        with Path(temp_file).open(mode="wb") as f:
            for chunk in r.iter_content(chunk_size=(16 * 1024 * 1024)):
                f.write(chunk)

    return str(temp_file.resolve())


# if __name__ == "__main__":
#     # This is for testing only
#     # bulk_files_df = bulk_downloads_webpage()
#     # bulk_files_df = bulk_files_df.head(2)
#     # ic(bulk_files_df)

#     download_zip_files(bulk_files_df)


# Assets
ew_census = EnglandAndWales()
country_metadata = ew_census.create_country_metadata()
data_publisher = ew_census.create_data_publisher()
geometry = ew_census.create_geometry()
source_data_releases = ew_census.create_source_data_releases()
catalog = ew_census.create_catalog()
census_tables = ew_census.create_census_tables()
source_metric_metadata = ew_census.create_source_metric_metadata()
derived_metrics = ew_census.create_derived_metrics()
metrics = ew_census.create_metrics()

