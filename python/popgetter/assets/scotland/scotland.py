import subprocess
import requests
import zipfile
import os
import urllib
import pandas as pd
import geopandas
import numpy as np
import matplotlib.pyplot as plt

from dagster import asset

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


class Scotland:
    cache_dir: str
    lookup: pd.DataFrame

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
    SCGeoCodes = ["CA", "DZ", "OA"]

    def __init__(self, cache_dir: str = "./cache/"):
        """Init and get lookup."""
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        lookup_path = download_file(self.cache_dir, self.URL_LOOKUP)
        self.lookup = pd.read_excel(lookup_path, sheet_name="OA_DZ_IZ_2011 Lookup")

    def __source_to_zip(self, source_name: str) -> str:
        """Downloads if necessary and returns the name of the locally cached zip file
        of the source data (replacing spaces with _)"""
        file_name = os.path.join(self.cache_dir, source_name.replace(" ", "_") + ".zip")
        if not os.path.isfile(file_name):
            if source_name.split()[0] == "Council":
                scotland_src = (
                    self.URL1
                    + "media/hjmd0oqr/"
                    + source_name.lower().replace(" ", "-")
                    + ".zip"
                )
            else:
                scotland_src = self.URL2 + urllib.parse.quote(source_name) + ".zip"
        return download_file(self.cache_dir, scotland_src, file_name)

    def get_rawdata(self, table: str, resolution: str) -> pd.DataFrame:
        """Gets the raw csv data and metadata."""
        if not os.path.exists(os.path.join(self.cache_dir, table + ".csv")):
            try:
                zf = self.__source_to_zip(
                    self.data_sources[self.GeoCodeLookup[resolution]]
                )
                with zipfile.ZipFile(zf) as zip_ref:
                    zip_ref.extractall(self.cache_dir)
            except NotImplementedError as _:
                subprocess.run(["unzip", "-o", zf, "-d", self.cache_dir])

        return pd.read_csv(os.path.join(self.cache_dir, table + ".csv"))

    def get_lc1117sc(self) -> pd.DataFrame:
        """Gets LC1117SC age by sex table at OA11 resolution."""
        df = self.get_rawdata("LC1117SC", "OA11").rename(
            columns={"Unnamed: 0": "OA11", "Unnamed: 1": "Age bracket"}
        )
        return df.loc[df["OA11"].isin(self.lookup["OutputArea2011Code"])]

    def get_shapefile(self) -> geopandas.GeoDataFrame:
        """Gets the shape file for OA11 resolution."""
        file_name = download_file(self.cache_dir, self.URL_SHAPEFILE)
        geo = geopandas.read_file(f"zip://{file_name}")
        return geo[geo["geo_code"].isin(self.lookup["OutputArea2011Code"])]

@asset
def download_data():
    cache_dir = "./cache/"
    scotland = Scotland(cache_dir)

@asset
def download_census() -> pd.DataFrame:
    cache_dir = "./cache/"
    scotland = Scotland(cache_dir)
    return scotland.get_lc1117sc()


@asset
def download_shapefile() -> geopandas.GeoDataFrame:
    cache_dir = "./cache/"
    scotland = Scotland(cache_dir)
    return scotland.get_shapefile()

# @multi_asset(
#     ins={
#         "individual_census_table": AssetIn(
#             key_prefix=asset_prefix, partition_mapping=needed_dataset_mapping
#         ),
#         # "individual_census_table": AssetIn(key_prefix=asset_prefix),
#         "filter_needed_catalog": AssetIn(key_prefix=asset_prefix),
#     },
# def generate_plots():
#     geo.merge(pop, left_on="geo_code", right_on="OA11", how="left")
#     # Plot
#     merged["log10 people"] = np.log10(merged["All people"])
#     merged[merged["Age bracket"] == "All people"].plot(
#         column="log10 people", legend=True
#     )
#     plt.show()


def main():
    cache_dir = "./cache/"

    # Make instance of Scotland
    scotland = Scotland(cache_dir)

    # Get OA11 Age/Sex data
    pop = scotland.get_lc1117sc()

    # Get shape file
    geo = scotland.get_shapefile()

    # Merge
    merged = geo.merge(pop, left_on="geo_code", right_on="OA11", how="left")
    print(merged)

    # Plot
    merged["log10 people"] = np.log10(merged["All people"])
    merged[merged["Age bracket"] == "All people"].plot(
        column="log10 people", legend=True
    )
    plt.show()


if __name__ == "__main__":
    main()
