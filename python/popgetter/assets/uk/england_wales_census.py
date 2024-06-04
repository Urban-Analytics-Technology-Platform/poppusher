from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urljoin

import pandas as pd
import requests
from icecream import ic

from popgetter.utils import SourceDataAssumptionsOutdated, extract_main_file_from_zip

# TODO:
# - Create a asset which is a catalog of the available data / tables / metrics
# - This catalog must include a field which is the smallest geometry level where the data is available
# - The geometry level is only discoverable after downloading the zip file
# - The zip files can contain multiple CSV files, one for each geometry level
# - Some of the downloaded files mistakenly have two consecutive `.` in the filename, e.g. `census2021-ts002-lsoa..csv`. We need to be able to gracefully handle this
# - The catalog must to parsed into an Dagster Partition, so that
#    - individual tables can be uploaded to the cloud table sensor
#    - the metadata object can be created for each table/metric


def get_bulk_zip_files():
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
    return _expand_tuples_in_df(download_df)


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


def download_zip_files(bulk_zip_files):
    """
    WIP: Download the bulk zip files from the bulk downloads page.
    """
    for index, row in bulk_zip_files.iterrows():
        with TemporaryDirectory() as temp_dir:
            temp_zip = Path(_download_zipfile(row["original_release_url"], temp_dir))

            for geom, csv_filename in _guess_csv_filename(
                row["original_release_filename"]
            ):
                ic(geom, csv_filename)

                extract_file_path = extract_main_file_from_zip(
                    temp_zip, Path(temp_dir), csv_filename
                )
                ic(extract_file_path)
                df = pd.read_csv(extract_file_path)

                ic(df.head())


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
        yield (geom, f"{stem}-{geom}.csv")


def _download_zipfile(source_download_url, temp_dir) -> str:
    temp_dir = Path(temp_dir)
    temp_file = temp_dir / "data.zip"

    with requests.get(source_download_url, stream=True) as r:
        r.raise_for_status()
        with Path(temp_file).open(mode="wb") as f:
            for chunk in r.iter_content(chunk_size=(16 * 1024 * 1024)):
                f.write(chunk)

    return str(temp_file.resolve())


if __name__ == "__main__":
    # This is for testing only
    bulk_files_df = get_bulk_zip_files()
    bulk_files_df = bulk_files_df.head(2)
    # ic(bulk_files_df)

    download_zip_files(bulk_files_df)
