import base64
from io import BytesIO
import subprocess
import tempfile
from typing import Tuple
from popgetter.utils import markdown_from_plot
import requests
import zipfile_deflate64 as zipfile
import os
import urllib.parse as urlparse
import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
from icecream import ic
import popgetter
from dagster import (
    AssetIn,
    AssetKey,
    AssetOut,
    DynamicPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    Output,
    Partition,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
    multi_asset,
    op,
)

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
URL_METADATA_INDEX = "https://www.scotlandscensus.gov.uk/media/kqcmo4ge/census-table-index-2011.xlsm"

data_sources = ["Council Area blk", "SNS Data Zone 2011 blk", "Output Area blk"]
GeoCodeLookup = {
    "LAD": 0,  # "Council Area blk"
    # MSOA (intermediate zone)?
    "LSOA11": 1,  # "SNS Data Zone 2011 blk"
    "OA11": 2,  # "Output Area blk"
}

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
    file_name: str | None = None,
    headers: dict[str, str] = HEADERS,
) -> str:
    """Downloads file checking first if exists in cache, returning file name."""
    file_name = (
        os.path.join(cache_dir, url.split("/")[-1]) if file_name is None else file_name
    )
    if not os.path.exists(file_name):
        r = requests.get(url, allow_redirects=True, headers=headers)
        open(file_name, "wb").write(r.content)
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
    os.makedirs(cache_dir, exist_ok=True)
    lookup_path = download_file(cache_dir, URL_LOOKUP)
    df1 = pd.read_excel(lookup_path, sheet_name="OA_DZ_IZ_2011 Lookup")
    df2 = pd.read_excel(lookup_path, sheet_name="DataZone2011Lookup")
    df3 = pd.read_excel(lookup_path, sheet_name="IntermediateZone2011Lookup")
    return df1, df2, df3


def source_to_zip(source_name: str, url: str) -> str:
    """Downloads if necessary and returns the name of the locally cached zip file
    of the source data (replacing spaces with _)"""
    file_name = os.path.join(cache_dir, source_name.replace(" ", "_") + ".zip")
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
def metadata_index(context) -> pd.DataFrame:
    dfs = pd.read_excel(
        URL_METADATA_INDEX,
        sheet_name=None,
        storage_options={"User-Agent": "Mozilla/5.0"},
    )
    df = dfs["Index"]
    add_metadata(context, df, "Metadata for census tables")
    return df


@asset
def catalog(context) -> pd.DataFrame:
    """Creates a catalog of the individual census tables from all data sources."""
    records = []
    for data_source in DATA_SOURCES:
        resolution = data_source["resolution"]
        source = data_source["source"]
        url = data_source["url"]
        with zipfile.ZipFile(source_to_zip(source, url)) as zip_ref:
            for name in zip_ref.namelist():
                record = {
                    "resolution": resolution,
                    "source": source,
                    "url": url,
                    "file_name": name,
                }
                context.log.debug(record)
                records.append(record)
                zip_ref.extract(name, cache_dir)

    # TODO: check if required
    for partition in context.instance.get_dynamic_partitions(PARTITIONS_DEF_NAME):
        context.instance.delete_dynamic_partition(PARTITIONS_DEF_NAME, partition)

    # Create a dynamic partition for the datasets listed in the catalog
    catalog_df: pd.DataFrame = pd.DataFrame.from_records(records)
    catalog_df["partition_keys"] = catalog_df[["resolution", "file_name"]].agg(
        lambda s: "/".join(s).rsplit(".")[0], axis=1
    )
    # TODO: consider filtering here based on a set of keys to keep derived from
    # config (i.e. backend/frontend modes)
    context.instance.add_dynamic_partitions(
        partitions_def_name=PARTITIONS_DEF_NAME,
        # To ensure this is unique, prepend the resolution
        partition_keys=catalog_df["partition_keys"].to_list(),
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
    df = pd.read_csv(os.path.join(cache_dir, table_details["file_name"].iloc[0]))
    add_metadata(context, df, table_details["partition_keys"].iloc[0])
    return df


@asset(partitions_def=dataset_node_partition)
def individual_census_table(context, catalog: pd.DataFrame) -> pd.DataFrame:
    """Creates individual census tables as dataframe."""
    partition_key = context.asset_partition_key_for_output()
    context.log.info(partition_key)
    table_details = catalog.loc[catalog["partition_keys"].isin([partition_key])]
    context.log.info(table_details)
    return get_table(context, table_details)


_subset = [
    {
        "partition_keys": "OA11/LC1117SC",
    },
]
_subset_partition_keys: list[str] = [r["partition_keys"] for r in _subset]
subset_mapping = SpecificPartitionsPartitionMapping(_subset_partition_keys)
subset_partition = StaticPartitionsDefinition(_subset_partition_keys)


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
    df = individual_census_table.rename(
        columns={"Unnamed: 0": "OA11", "Unnamed: 1": "Age bracket"}
    )
    df = df.loc[df["OA11"].isin(oa_dz_iz_2011_lookup["OutputArea2011Code"])]
    add_metadata(context, df, _subset_partition_keys)
    return df


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
