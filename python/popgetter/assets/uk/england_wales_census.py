from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urljoin

from dagster import DynamicPartitionsDefinition, MetadataValue, asset
import pandas as pd
import requests
from icecream import ic

from popgetter.utils import SourceDataAssumptionsOutdated, extract_main_file_from_zip
from popgetter.metadata import MetricMetadata, DataPublisher

# TODO:
# - Create a asset which is a catalog of the available data / tables / metrics
# - This catalog must include a field which is the smallest geometry level where the data is available
# - The geometry level is only discoverable after downloading the zip file
# - The zip files can contain multiple CSV files, one for each geometry level
# - Some of the downloaded files mistakenly have two consecutive `.` in the filename, e.g. `census2021-ts002-lsoa..csv`. We need to be able to gracefully handle this
# - The catalog must to parsed into an Dagster Partition, so that
#    - individual tables can be uploaded to the cloud table sensor
#    - the metadata object can be created for each table/metric

from .united_kingdom import country

# TODO - add proper details here
publisher: DataPublisher = DataPublisher(
    name="ONS - fix me!",
    url="https://www.nomisweb.co.uk/sources/census_2021_bulk",
    description="ONS - fix me!",
    countries_of_interest=[country.id],
)


bulk_tables_partition = DynamicPartitionsDefinition(name="bulk_tables")
from .united_kingdom import asset_prefix


@asset(partitions_def=bulk_tables_partition, key_prefix=asset_prefix,
        description="Table of available bulk downloads from the Census 2021 website.")
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

    download_df = dfs[0]
    download_df.columns = columns

    # There are some subheadings in the table, which are added as rows by `read_html`
    # These can be identified by the `table_id` == `description` == `original_release_filename`
    # We need to drop these rows
    download_df = download_df[download_df["table_id"] != download_df["description"]]

    expanded_df = _expand_tuples_in_df(download_df)

    # Update the relevant partitions
    # First delete the old dynamic partitions from the previous run
    # for partition in context.instance.get_dynamic_partitions("bulk_tables"):
    #     context.instance.delete_dynamic_partition("bulk_tables", partition)

    table_ids = expanded_df["table_id"].to_list()
    ic(table_ids)
    context.instance.add_dynamic_partitions(
        partitions_def_name="bulk_tables", partition_keys=table_ids
    )

    # Add some metadata to the context
    metadata = {
        "title": "Table of available bulk downloads from the Census 2021 website.",
        "num_records": len(expanded_df),  # Metadata can be any key-value pair
        "columns": MetadataValue.md(
            "\n".join([f"- '`{col}`'" for col in expanded_df.columns.to_list()])
        ),
        "preview": MetadataValue.md(
            expanded_df.to_markdown()
        ),
    }

    context.add_output_metadata(metadata=metadata)

    return expanded_df


def _expand_tuples_in_df(df) -> pd.DataFrame:
    """
    Expand the tuples in the DataFrame.
    """
    root_url = "https://www.nomisweb.co.uk/"

    columns = [
        "table_id",
        "description",
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
    new_df["description"] = df["description"].apply(lambda x: x[0])
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


@asset(partitions_def=bulk_tables_partition, key_prefix=asset_prefix)
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

    current_table = bulk_downloads_webpage[bulk_downloads_webpage["table_id"] == table_id]
    ic(current_table)

    description = current_table["description"].values[0]
    original_release_url = current_table["original_release_url"].values[0]
    original_release_filename = current_table["original_release_filename"].values[0]

    all_columns = []

    with TemporaryDirectory() as temp_dir:
        ic(original_release_url)
        temp_zip = Path(_download_zipfile(original_release_url, temp_dir))

        for geom, csv_filebase in _guess_csv_filename(
            original_release_filename
        ):
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

    columns_df = pd.DataFrame(all_columns, columns=["table_id", "description", "geom", "column_name"])

    # Add some metadata to the context
    metadata = {
        "title": "Table of available bulk downloads from the Census 2021 website.",
        "num_records": len(columns_df),  # Metadata can be any key-value pair
        "columns": MetadataValue.md(
            "\n".join([f"- '`{col}`'" for col in columns_df.columns.to_list()])
        ),
        "preview": MetadataValue.md(
            columns_df.to_markdown()
        ),
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

def _guess_csv_filename(zip_filename):
    """
    Guess the name of the main file in the zip file.
    """
    stem = Path(zip_filename).stem

    # The order of the geometries is from smallest to largest
    # TODO: Add descriptions to these abbreviations
    geom_by_size = [
        "oa",
        "lsoa",
        "msoa",
        "ltla",
        "rgn",
        "ctry",
    ]

    for geom in geom_by_size:
        yield (geom, f"{stem}-{geom}")


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
