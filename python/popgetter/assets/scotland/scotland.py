from __future__ import annotations

import urllib.parse as urlparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import zipfile_deflate64 as zipfile
from dagster import (
    AssetIn,
    AssetOut,
    DynamicPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
    multi_asset,
)

from popgetter.utils import markdown_from_plot

"""
Notes:
  - 2011 data using UKCensusAPI, 2022 data expected soon given recent initial
    publication
  - Reusing some bits of code from UKCensusAPI:
    https://github.com/alan-turing-institute/UKCensusAPI/blob/master/ukcensusapi/NRScotland.py
"""


PARTITIONS_DEF_NAME = "dataset_tables"
dataset_node_partition = DynamicPartitionsDefinition(name=PARTITIONS_DEF_NAME)

# cache_dir = tempfile.mkdtemp()
cache_dir = "./cache"

URL = "https://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html"
URL1 = "https://www.scotlandscensus.gov.uk/"
URL2 = "https://nrscensusprodumb.blob.core.windows.net/downloads/"
URL_LOOKUP = (
    "https://www.nrscotland.gov.uk/files//geography/2011-census/OA_DZ_IZ_2011.xlsx"
)
URL_SHAPEFILE = "https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/infuse_oa_lyr_2011.zip"
URL_CATALOG_METADATA = (
    "https://www.scotlandscensus.gov.uk/media/kqcmo4ge/census-table-index-2011.xlsm"
)

data_sources = ["Council Area blk", "SNS Data Zone 2011 blk", "Output Area blk"]
GeoCodeLookup = {
    "LAD": 0,  # "Council Area blk"
    # MSOA (intermediate zone)?
    "LSOA11": 1,  # "SNS Data Zone 2011 blk"
    "OA11": 2,  # "Output Area blk"
}

# From: https://github.com/alan-turing-institute/microsimulation/blob/37ce2843f10b83a8e7a225c801cec83b85e6e0d0/microsimulation/common.py#L32
REQUIRED_TABLES = [
    "QS103SC",
    "QS104SC",
    "KS201SC",
    "DC1117SC",
    "DC2101SC",
    "DC6206SC",
    "LC1117SC",
]
REQUIRED_TABLES_REGEX = "|".join(REQUIRED_TABLES)

DATA_SOURCES = [
    {
        "source": "Council Area blk",
        "resolution": "LAD",
        "url": URL1 + "/media/hjmd0oqr/council-area-blk.zip",
    },
    {
        "source": "SNS Data Zone 2011 blk",
        "resolution": "LSOA11",
        "url": URL2 + urlparse.quote("SNS Data Zone 2011 blk") + ".zip",
    },
    {
        "source": "Output Area blk",
        "resolution": "OA11",
        "url": URL2 + urlparse.quote("Output Area blk") + ".zip",
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0"
}


def download_file(
    cache_dir: str,
    url: str,
    file_name: Path | None = None,
    headers: dict[str, str] = HEADERS,
) -> Path:
    """Downloads file checking first if exists in cache, returning file name."""
    file_name = Path(cache_dir) / url.split("/")[-1] if file_name is None else file_name
    if not Path(file_name).exists():
        r = requests.get(url, allow_redirects=True, headers=headers)
        with Path(file_name).open("wb") as fp:
            fp.write(r.content)
    return file_name


# NB. Make sure no spaces in asset keys
@multi_asset(
    outs={
        "oa_dz_iz_2011_lookup": AssetOut(),
        "data_zone_2011_lookup": AssetOut(),
        "intermediate_zone_2011_lookup": AssetOut(),
    },
)
def lookups():
    """Creates lookup dataframes."""
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    lookup_path = download_file(cache_dir, URL_LOOKUP)
    df1 = pd.read_excel(lookup_path, sheet_name="OA_DZ_IZ_2011 Lookup")
    df2 = pd.read_excel(lookup_path, sheet_name="DataZone2011Lookup")
    df3 = pd.read_excel(lookup_path, sheet_name="IntermediateZone2011Lookup")
    return df1, df2, df3


def source_to_zip(source_name: str, url: str) -> Path:
    """Downloads if necessary and returns the name of the locally cached zip file
    of the source data (replacing spaces with _)"""
    file_name = Path(cache_dir) / (source_name.replace(" ", "_") + ".zip")
    return download_file(cache_dir, url, file_name)


def add_metadata(context, df: pd.DataFrame | gpd.GeoDataFrame, title: str | list[str]):
    context.add_output_metadata(
        metadata={
            "title": title,
            "num_records": len(df),
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in df.columns.to_list()])
            ),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset
def catalog_metadata(context) -> pd.DataFrame:
    catalog_metadata_df = pd.read_excel(
        URL_CATALOG_METADATA,
        sheet_name=None,
        header=None,
        storage_options={"User-Agent": "Mozilla/5.0"},
    )["Index"].rename(
        columns={
            0: "census_release",
            1: "table_name",
            2: "description",
            3: "population_coverage",
            4: "variable",
            5: "catalog_resolution",
            6: "year",
            7: "additional_url",
            8: "population_coverage_and_variable",
        }
    )
    add_metadata(context, catalog_metadata_df, "Metadata for census tables")
    return catalog_metadata_df


def get_table_metadata(
    catalog_metadata: pd.DataFrame, table_name: str
) -> dict[str, str]:
    """Returns a dict of table metadata for a given table name."""
    rows = catalog_metadata.loc[catalog_metadata.loc[:, "table_name"].eq(table_name)]
    census_release = rows.loc[:, "description"].unique()[0]
    description = rows.loc[:, "description"].unique()[0]
    population_coverage = rows.loc[:, "description"].unique()[0]
    variables = ", ".join(rows.loc[:, "variable"].astype(str).to_list())
    catalog_resolution = rows.loc[:, "catalog_resolution"].unique()[0]
    year = int(rows.loc[:, "year"].unique()[0])
    return {
        "census_release": census_release,
        "description": description,
        "population_coverage": population_coverage,
        "variables": variables,
        "catalog_resolution": catalog_resolution,
        "year": str(year),
        "human_readable_name": f"{description} ({population_coverage})",
    }


def get_table_name(file_name: str) -> str:
    return file_name.rsplit(".csv")[0]


@asset
def catalog(context, catalog_metadata: pd.DataFrame) -> pd.DataFrame:
    """Creates a catalog of the individual census tables from all data sources."""
    records = []
    for data_source in DATA_SOURCES:
        resolution = data_source["resolution"]
        source = data_source["source"]
        url = data_source["url"]
        zip_file_name = source_to_zip(source, url)
        with zipfile.ZipFile(zip_file_name) as zip_ref:
            for file_name in zip_ref.namelist():
                # Get table name
                table_name = get_table_name(file_name)

                # Skip bulk output files and missing tables from catalog_metadata
                if (
                    "bulk_output" in file_name.lower()
                    or catalog_metadata.loc[:, "table_name"].ne(table_name).all()
                ):
                    continue

                # Get table metadata
                table_metadata = get_table_metadata(catalog_metadata, table_name)

                # Create a record for each census table use same keys as MetricMetadata
                # where possible since this makes it simpler to populate derived
                # metrics downstream
                record = {
                    "resolution": resolution,
                    "catalog_resolution": table_metadata["catalog_resolution"],
                    "source": source,
                    "url": url,
                    "file_name": Path(source) / file_name,
                    "table_name": table_name,
                    "year": table_metadata["year"],
                    # Use constructed name of description and coverage
                    "human_readable_name": table_metadata["human_readable_name"],
                    "source_metric_id": None,
                    # Use catalog_metadata description
                    "description": table_metadata["description"],
                    "hxl_tag": None,
                    "metric_parquet_file_url": None,
                    "parquet_column_name": None,
                    "parquet_margin_of_error_column": None,
                    "parquet_margin_of_error_file": None,
                    "potential_denominator_ids": None,
                    "parent_metric_id": None,
                    # TODO: check this is not an ID but a name
                    "source_data_release_id": table_metadata["census_release"],
                    "source_download_url": url,
                    # TODO: what should this be?
                    "source_archive_file_path": None,
                    "source_documentation_url": URL_CATALOG_METADATA,
                }
                context.log.debug(record)
                records.append(record)
                zip_ref.extract(file_name, Path(cache_dir) / source)

    # TODO: check if required
    for partition in context.instance.get_dynamic_partitions(PARTITIONS_DEF_NAME):
        context.instance.delete_dynamic_partition(PARTITIONS_DEF_NAME, partition)

    # Create a dynamic partition for the datasets listed in the catalog
    catalog_df: pd.DataFrame = pd.DataFrame.from_records(records)
    catalog_df["partition_keys"] = (
        catalog_df[["year", "resolution", "table_name"]]
        .astype(str)
        .agg(lambda s: "/".join(s).rsplit(".")[0], axis=1)
    )
    # TODO: consider filtering here based on a set of keys to keep derived from
    # config (i.e. backend/frontend modes)
    context.instance.add_dynamic_partitions(
        partitions_def_name=PARTITIONS_DEF_NAME,
        # To ensure this is unique, prepend the resolution,
        partition_keys=catalog_df.loc[
            catalog_df["partition_keys"].str.contains(REQUIRED_TABLES_REGEX),
            "partition_keys",
        ].to_list(),
    )
    context.add_output_metadata(
        metadata={
            "num_records": len(catalog_df),
            "ignored_datasets": "",
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in catalog_df.columns.to_list()])
            ),
            "columns_types": MetadataValue.md(catalog_df.dtypes.to_markdown()),
            "preview": MetadataValue.md(catalog_df.to_markdown()),
        }
    )
    return catalog_df


def get_table(context, table_details) -> pd.DataFrame:
    table_df = pd.read_csv(Path(cache_dir) / table_details["file_name"].iloc[0])
    add_metadata(context, table_df, table_details["partition_keys"].iloc[0])
    return table_df


@asset(partitions_def=dataset_node_partition)
def individual_census_table(context, catalog: pd.DataFrame) -> pd.DataFrame:
    """Creates individual census tables as dataframe."""
    partition_key = context.asset_partition_key_for_output()
    context.log.info(partition_key)
    table_details = catalog.loc[catalog["partition_keys"].isin([partition_key])]
    context.log.info(table_details)
    return get_table(context, table_details)


subset_partition_keys: list[str] = ["2011/OA11/LC1117SC"]
subset_mapping = SpecificPartitionsPartitionMapping(subset_partition_keys)
subset_partition = StaticPartitionsDefinition(subset_partition_keys)


# TODO: revise to include all partitions and extract column name for metadata from catalog
@multi_asset(
    ins={
        "individual_census_table": AssetIn(partition_mapping=subset_mapping),
    },
    outs={
        "oa11_lc1117sc": AssetOut(),
    },
    partitions_def=dataset_node_partition,
)
def oa11_lc1117sc(
    context, individual_census_table, oa_dz_iz_2011_lookup
) -> pd.DataFrame:
    """Gets LC1117SC age by sex table at OA11 resolution."""
    derived_census_table = individual_census_table.rename(
        columns={"Unnamed: 0": "OA11", "Unnamed: 1": "Age bracket"}
    )
    derived_census_table = derived_census_table.loc[
        derived_census_table["OA11"].isin(oa_dz_iz_2011_lookup["OutputArea2011Code"])
    ]
    add_metadata(context, derived_census_table, subset_partition_keys)
    return derived_census_table


@asset
def geometry(context, oa_dz_iz_2011_lookup) -> gpd.GeoDataFrame:
    """Gets the shape file for OA11 resolution."""
    file_name = download_file(cache_dir, URL_SHAPEFILE)
    geo = gpd.read_file(f"zip://{file_name}")
    add_metadata(context, geo, "Geometry file")
    return geo[geo["geo_code"].isin(oa_dz_iz_2011_lookup["OutputArea2011Code"])]


@multi_asset(
    ins={
        "oa11_lc1117sc": AssetIn(partition_mapping=subset_mapping),
        "geometry": AssetIn(partition_mapping=subset_mapping),
    },
    outs={
        "plot": AssetOut(),
    },
    partitions_def=dataset_node_partition,
)
def plot(geometry: gpd.GeoDataFrame, oa11_lc1117sc: pd.DataFrame):
    """Plots map with log density of people."""
    merged = geometry.merge(
        oa11_lc1117sc, left_on="geo_code", right_on="OA11", how="left"
    )
    merged["log10 people"] = np.log10(merged["All people"])
    merged[merged["Age bracket"] == "All people"].plot(
        column="log10 people", legend=True
    )
    md_content = markdown_from_plot(plt)
    return MaterializeResult(metadata={"plot": MetadataValue.md(md_content)})
