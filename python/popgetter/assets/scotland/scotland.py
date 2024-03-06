import subprocess
import tempfile
from typing import Tuple
import requests
# import zipfile
import zipfile_deflate64 as zipfile
import os
import urllib.parse as urlparse
import pandas as pd
import geopandas
import numpy as np
import matplotlib.pyplot as plt
from icecream import ic
from dagster import AssetIn, AssetOut, DynamicPartitionsDefinition, MetadataValue, Output, SpecificPartitionsPartitionMapping, StaticPartitionsDefinition, asset, multi_asset

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

data_sources = ["Council Area blk", "SNS Data Zone 2011 blk", "Output Area blk"]
GeoCodeLookup = {
    "LAD": 0,  # "Council Area blk"
    # MSOA (intermediate zone)?
    "LSOA11": 1,  # "SNS Data Zone 2011 blk"
    "OA11": 2,  # "Output Area blk"
}
# SCGeoCodes = ["CA", "DZ", "OA"]


DATA_SOURCES = {
    0: {
        "source": "Council Area blk",
        "resolution": "LAD",
        "url": URL1 + "/media/hjmd0oqr/council-area-blk.zip"
    },
    1: {
        "source": "SNS Data Zone 2011 blk",
        "resolution": "LSOA11",
        "url": URL2 + urlparse.quote("SNS Data Zone 2011 blk") + ".zip"
    },
    2: {
        "source": "Output Area blk",
        "resolution": "OA11",
        "url": URL2 + urlparse.quote("Output Area blk") + ".zip"
    }
}


# NB. Make sure no spaces in asset keys
@multi_asset(
    outs={
        "oa_dz_iz_2011_lookup": AssetOut(),
        "data_zone_2011_lookup": AssetOut(),
        "intermediate_zone_2011_lookup": AssetOut(),
    },
)
def download_lookup():
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

@asset
def make_catalog(context) -> pd.DataFrame:
    records = []
    for data_source in DATA_SOURCES.values():
        resolution = data_source["resolution"]
        source = data_source["source"]
        url = data_source["url"]
        with zipfile.ZipFile(source_to_zip(source, url)) as zip_ref: 
            for name in zip_ref.namelist():
                print(name)
                record = {
                        "resolution": resolution,
                        "source": source,
                        "url": url,
                        "file_name": name,
                    }
                records.append(record)
                ic(record)
                zip_ref.extract(name, cache_dir)
    
    for partition in context.instance.get_dynamic_partitions(PARTITIONS_DEF_NAME):
        context.instance.delete_dynamic_partition(PARTITIONS_DEF_NAME, partition)

    # Create a dynamic partition for the datasets listed in the catalog
    catalog_df: pd.DataFrame = pd.DataFrame.from_records(records)
    partition_keys = catalog_df["file_name"].to_list()
    context.instance.add_dynamic_partitions(
        partitions_def_name=PARTITIONS_DEF_NAME, partition_keys=partition_keys
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
    context.add_output_metadata(
        metadata={
            "title": table_details["file_nae"].iloc[0],
            # "title": "Test",
            "num_records": len(df),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in df.columns.to_list()])
            ),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )
    return df

@asset(partitions_def=dataset_node_partition)
def individual_census_table(context, make_catalog: pd.DataFrame) -> pd.DataFrame:
    partition_key = context.asset_partition_key_for_output()
    ic(partition_key)
    row = make_catalog.loc[make_catalog["file_name"].isin([partition_key])]
    ic(row)
    return get_table(context, table_details=row)


# # TODO: add to derived
# def get_lc1117sc(context, lookup, ) -> pd.DataFrame:
#     """Gets LC1117SC age by sex table at OA11 resolution."""
#     df = get_rawdata("LC1117SC", "OA11").rename(
#         columns={"Unnamed: 0": "OA11", "Unnamed: 1": "Age bracket"}
#     )
#     return df.loc[df["OA11"].isin(lookup["OutputArea2011Code"])]


# # TODO: add shapefile
# def shapefile(context) -> geopandas.GeoDataFrame:
#     """Gets the shape file for OA11 resolution."""
#     file_name = download_file(cache_dir, URL_SHAPEFILE)
#     geo = geopandas.read_file(f"zip://{file_name}")
#     return geo[geo["geo_code"].isin(lookup["OutputArea2011Code"])]

    
# # TODO: add plots
# @asset
# def generate_plots():
#     geo.merge(pop, left_on="geo_code", right_on="OA11", how="left")
#     # Plot
#     merged["log10 people"] = np.log10(merged["All people"])
#     merged[merged["Age bracket"] == "All people"].plot(
#         column="log10 people", legend=True
#     )
#     plt.show()
