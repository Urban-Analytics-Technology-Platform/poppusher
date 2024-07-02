#!/usr/bin/python3
from __future__ import annotations

import tempfile
import urllib.parse as urlparse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from functools import reduce
from pathlib import Path
from typing import ClassVar

import geopandas as gpd
import pandas as pd
import zipfile_deflate64 as zipfile
from dagster import (
    MetadataValue,
    asset,
)
from icecream import ic

from popgetter.assets.country import Country
from popgetter.assets.gb_sct.utils import HEADERS, download_file
from popgetter.cloud_outputs import (
    GeometryOutput,
    MetricsOutput,
    send_to_geometry_sensor,
)
from popgetter.metadata import (
    COL,
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
    metadata_to_dataframe,
)
from popgetter.utils import add_metadata, markdown_from_plot

# Currently including only releases matching tables included
GENERAL_METHODS_URL = "https://www.scotlandscensus.gov.uk/media/jx2lz54n/scotland-s_census_2011_general_report.pdf"
CENSUS_REFERENCE_DATE = date(2011, 3, 27)
CENSUS_COLLECTION_DATE = date(2011, 3, 27)
CENSUS_EXPECT_NEXT_UPDATE = date(2022, 1, 1)


# Source releases for 2011:
# '3A','3I', '2A', '3C', '3D', '3E', '3L', '3K', '3N',
# '3B', '3J', '3M', '3G', '3H', '2C', '2B', '2D',
# Others:
# '2001 Census',
# 'nan', '75+', 'Daytime Tables',
# '1991 Census', '1992 Census',
SOURCE_DATA_RELEASES: dict[str, SourceDataRelease] = {
    "3A": SourceDataRelease(
        name="Census 2011: Release 3A",
        date_published=date(2014, 2, 27),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3a",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3I": SourceDataRelease(
        name="Census 2011: Release 3I",
        date_published=date(2014, 9, 24),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3i",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "2A": SourceDataRelease(
        name="Census 2011: Release 2A",
        date_published=date(2013, 9, 26),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2013/census-2011-release-2a",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3C": SourceDataRelease(
        name="Census 2011: Release 3C",
        date_published=date(2014, 4, 9),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-releases-2d-and-3c",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3D": SourceDataRelease(
        name="Census 2011: Release 3D",
        date_published=date(2014, 5, 15),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3d",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3E": SourceDataRelease(
        name="Census 2011: Release 3E",
        date_published=date(2014, 6, 4),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3e",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3F": SourceDataRelease(
        name="Census 2011: Release 3F",
        date_published=date(2014, 6, 25),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-release-3f",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3L": SourceDataRelease(
        name="Census 2011: Release 3L",
        date_published=date(2014, 11, 27),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3l",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3K": SourceDataRelease(
        name="Census 2011: Release 3K",
        date_published=date(2014, 11, 6),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3k",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3N": SourceDataRelease(
        name="Census 2011: Release 3N",
        date_published=date(2015, 1, 29),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2015/census-2011-release-3n",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3B": SourceDataRelease(
        name="Census 2011: Release 3B",
        date_published=date(2014, 3, 19),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-detailed-characteristics-on-ethnicity-identity-language-and-religion-in-scotland-%E2%80%93-release-3b",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3J": SourceDataRelease(
        name="Census 2011: Release 3J",
        date_published=date(2014, 10, 16),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3j",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3M": SourceDataRelease(
        name="Census 2011: Release 3M",
        date_published=date(2014, 12, 18),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3m",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3G": SourceDataRelease(
        name="Census 2011: Release 3G",
        date_published=date(2014, 7, 23),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3g",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "3H": SourceDataRelease(
        name="Census 2011: Release 3H",
        date_published=date(2014, 8, 13),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3h",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "2C": SourceDataRelease(
        name="Census 2011: Release 2C",
        date_published=date(2013, 12, 18),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2013/census-2011-release-2c",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "2B": SourceDataRelease(
        name="Census 2011: Release 2B",
        date_published=date(2013, 11, 14),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2013/census-2011-release-2b",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "2D": SourceDataRelease(
        name="Census 2011: Release 2D",
        date_published=date(2014, 4, 9),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-releases-2d-and-3c",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
    "75+": SourceDataRelease(
        name="Census 2011: 75+",
        # TODO: unable to find published date for 75+ release
        date_published=date(2014, 1, 1),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="TBD",
        data_publisher_id="TBD",
        description="TBD",
        geometry_metadata_id="TBD",
    ),
}


# TODO: remove ones no longer used
URL = "https://www.scotlandscensus.gov.uk/ods-web/download/getDownloadFile.html"
URL1 = "https://www.scotlandscensus.gov.uk/"
URL2 = "https://nrscensusprodumb.blob.core.windows.net/downloads/"
URL_LOOKUP = (
    "https://www.nrscotland.gov.uk/files//geography/2011-census/OA_DZ_IZ_2011.xlsx"
)
URL_SHAPEFILE = "https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/infuse_oa_lyr_2011.zip"
URL_CATALOG = (
    "https://www.scotlandscensus.gov.uk/media/kqcmo4ge/census-table-index-2011.xlsm"
)


DATA_SOURCES = [
    {
        "source": "Council Area blk",
        # "resolution": "LAD",
        "resolution": "CouncilArea2011",
        "url": URL1 + "/media/hjmd0oqr/council-area-blk.zip",
    },
    {
        "source": "SNS Data Zone 2011 blk",
        # "resolution": "LSOA11",
        "resolution": "DataZone2011",
        "url": URL2 + urlparse.quote("SNS Data Zone 2011 blk") + ".zip",
    },
    {
        "source": "Output Area blk",
        # "resolution": "OA11",
        "resolution": "OutputArea2011",
        "url": URL2 + urlparse.quote("Output Area blk") + ".zip",
    },
]


@dataclass
class ScotlandGeometryLevel:
    level: str
    hxl_tag: str
    geo_id_column: str
    census_table_column: str
    name_columns: dict[str, str]  # keys = language codes, values = column names
    url: str
    lookup_url: str | None
    lookup_sheet: str | None
    left_on: str | None
    right_on: str | None


SCOTLAND_GEO_LEVELS = {
    "OutputArea2011": ScotlandGeometryLevel(
        level="OutputArea2011",
        hxl_tag="TBD",
        geo_id_column="OA_CODE",
        census_table_column="TODO",
        # census_table_column="Census 2021 Data Zone Code",
        name_columns={"eng": "OutputArea2011Name"},  # TODO
        # url=URL_SHAPEFILE,
        url="https://www.nrscotland.gov.uk/files/geography/output-area-2011-eor.zip",
        lookup_url=None,
        lookup_sheet=None,
        left_on="OA_CODE",
        right_on="OutputArea2011Code",
    ),
    # LSOA11
    "DataZone2011": ScotlandGeometryLevel(
        level="DataZone2011",
        hxl_tag="TBD",
        geo_id_column="DataZone",
        census_table_column="TODO",
        # census_table_column="Census 2021 Data Zone Code",
        name_columns={"eng": "Name"},
        url="https://maps.gov.scot/ATOM/shapefiles/SG_DataZoneBdry_2011.zip",
        lookup_url=None,
        lookup_sheet=None,
        left_on="DataZone",
        right_on="DataZone2011Code",
    ),
    # "MSOA11": ScotlandGeometryLevel(
    #     level="OA11",
    #     hxl_tag="TBD",
    #     geo_id_column="OA_CODE",
    #     census_table_column="TODO",
    #     # census_table_column="Census 2021 Data Zone Code",
    #     name_columns={"eng": "OA_CODE"},
    #     # url=URL_SHAPEFILE,
    #     url="https://www.nrscotland.gov.uk/files/geography/output-area-2011-eor.zip",
    #     lookup_url=None,
    #     lookup_sheet=None,
    #     left_on=None,
    #     right_on=None,
    # ),
    # LAD
    "CouncilArea2011": ScotlandGeometryLevel(
        level="CouncilArea2011",
        hxl_tag="TBD",
        geo_id_column="CouncilArea2011Code",
        census_table_column="TODO",
        # census_table_column="Census 2021 Data Zone Code",
        name_columns={"eng": "CouncilArea2011Name"},
        url="https://maps.gov.scot/ATOM/shapefiles/SG_DataZoneBdry_2011.zip",
        lookup_url=None,
        lookup_sheet=None,
        left_on="DataZone",
        right_on="DataZone2011Code",
    ),
}


# Use temporary directory
CACHE_DIR = tempfile.mkdtemp()


@dataclass
class DerivedColumn:
    hxltag: str
    filter_func: Callable[[pd.DataFrame], pd.DataFrame]
    output_column_name: str
    human_readable_name: str


@dataclass
class SourceTable:
    hxltag: str
    geo_level: str
    geo_column: str
    source_column: str


# Config for each partition to be derived
AGE_CODE = "`Age Category`"
SEX_LABEL = "`Sex Label`"
INFANTS = ["0 to 4"]
CHILDREN_5_TO_17 = ["5 to 9", "10 to 11", "12 to 14" "15", "16 to 17"]
CHILDREN = ["0 to 4", "5 to 9", "10 to 11", "12 to 14" "15", "16 to 17"]
ADULTS = ["18 to 19"] + [f"{i} to {i+4}" for i in range(20, 91, 5)] + ["95 and over"]
PEOPLE = ["All people"]
DERIVED_COLUMNS = [
    DerivedColumn(
        hxltag="#population+children+age5_17",
        filter_func=lambda df: df.query(f"{AGE_CODE} in @children_5_to_17"),
        output_column_name="children_5_17",
        human_readable_name="Children aged 5 to 17",
    ),
    DerivedColumn(
        hxltag="#population+infants+age0_4",
        filter_func=lambda df: df.query(f"{AGE_CODE} in @infants"),
        output_column_name="infants_0_4",
        human_readable_name="Infants aged 0 to 4",
    ),
    DerivedColumn(
        hxltag="#population+children+age0_17",
        filter_func=lambda df: df.query(f"{AGE_CODE} in @children"),
        output_column_name="children_0_17",
        human_readable_name="Children aged 0 to 17",
    ),
    DerivedColumn(
        hxltag="#population+adults+f",
        filter_func=lambda df: df.query(
            f"{AGE_CODE} in @adults and {SEX_LABEL} == 'Female'"
        ),
        output_column_name="adults_f",
        human_readable_name="Female adults",
    ),
    DerivedColumn(
        hxltag="#population+adults+m",
        filter_func=lambda df: df.query(
            f"{AGE_CODE} in @adults and {SEX_LABEL} == 'Male'"
        ),
        output_column_name="adults_m",
        human_readable_name="Male adults",
    ),
    DerivedColumn(
        hxltag="#population+adults",
        filter_func=lambda df: df.query(f"{AGE_CODE} in @adults"),
        output_column_name="adults",
        human_readable_name="Adults",
    ),
    DerivedColumn(
        hxltag="#population+ind",
        filter_func=lambda df: df.query(f"{AGE_CODE} in @people"),
        output_column_name="individuals",
        human_readable_name="Total individuals",
    ),
]

# For a subset:
# TABLES_TO_PROCESS: list[str] = [
#     "QS103SC",
#     "QS104SC",
#     "KS201SC",
#     "DC1117SC",
#     "DC2101SC",
#     "DC6206SC",
#     "LC1117SC",
# ]

# For all available:
TABLES_TO_PROCESS = None

DERIVED_COLUMN_SPECIFICATIONS: dict[str, list[DerivedColumn]] = {
    "2011/OutputArea2011/LC1117SC": DERIVED_COLUMNS,
    "2011/DataZone2011/LC1117SC": DERIVED_COLUMNS,
    "2011/CouncilArea2011/LC1117SC": DERIVED_COLUMNS,
}


def get_source_data_release(geo_level: str, cenesus_release: str) -> str:
    return geo_level + "_" + cenesus_release


class Scotland(Country):
    country_metadata: ClassVar[CountryMetadata] = CountryMetadata(
        name_short_en="Scotland",
        name_official="Scotland",
        iso3="GBR",
        iso2="GB",
        iso3166_2="GB-SCT",
    )
    geo_levels: ClassVar[list[str]] = list(SCOTLAND_GEO_LEVELS.keys())
    tables_to_process: list[str] | None = TABLES_TO_PROCESS
    allow_missing_derived_metrics: ClassVar[bool] = True

    def _catalog(self, context) -> pd.DataFrame:
        """Creates a catalog of the individual census tables from all data sources."""

        def source_to_zip(source_name: str, url: str) -> Path:
            """Downloads if necessary and returns the name of the locally cached zip file
            of the source data (replacing spaces with _)"""
            file_name = Path(CACHE_DIR) / (source_name.replace(" ", "_") + ".zip")
            return download_file(CACHE_DIR, url, file_name)

        def get_table_name(file_name: str) -> str:
            return file_name.rsplit(".csv")[0]

        def get_table_metadata(
            catalog_reference: pd.DataFrame, table_name: str
        ) -> dict[str, str]:
            """Returns a dict of table metadata for a given table name."""
            rows = catalog_reference.loc[
                catalog_reference.loc[:, "table_name"].eq(table_name)
            ]
            census_release = rows.loc[:, "census_release"].unique()[0]
            description = rows.loc[:, "description"].unique()[0]
            population_coverage = rows.loc[:, "population_coverage"].unique()[0]
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

        # Download catalog reference
        catalog_reference = pd.read_excel(
            URL_CATALOG,
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
        # Remove all keys
        self.remove_all_partition_keys(context)

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

                    # Skip bulk output files and missing tables from catalog_reference
                    if (
                        "bulk_output" in file_name.lower()
                        or catalog_reference.loc[:, "table_name"].ne(table_name).all()
                    ):
                        continue

                    # Get table metadata
                    table_metadata = get_table_metadata(catalog_reference, table_name)

                    # Get source release metadata if available
                    source_data_release = SOURCE_DATA_RELEASES.get(
                        table_metadata["census_release"], None
                    )
                    source_data_release_id = (
                        None if source_data_release is None else source_data_release.id
                    )

                    # Skip if not required
                    if (
                        self.tables_to_process is not None
                        and table_name not in self.tables_to_process
                    ):
                        continue

                    # Remove failing case (no data in census table):
                    #   "2011/DataZone2011/QS421SC"
                    if table_name == "QS421SC" and resolution == "DataZone2011":
                        continue

                    # Fix case with missing data for release
                    if resolution == "CouncilArea2011" and table_name == "DC6102SC":
                        table_metadata["census_release"] = "3I"

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
                        # Use catalog_reference description
                        "description": table_metadata["description"],
                        "hxl_tag": None,
                        "metric_parquet_file_url": None,
                        "parquet_column_name": None,
                        "parquet_margin_of_error_column": None,
                        "parquet_margin_of_error_file": None,
                        "potential_denominator_ids": None,
                        "parent_metric_id": None,
                        # TODO: check this is not an ID but a name
                        "source_data_release_id": source_data_release_id,
                        "census_release": table_metadata["census_release"],
                        "source_download_url": url,
                        # TODO: what should this be?
                        "source_archive_file_path": None,
                        "source_documentation_url": URL_CATALOG,
                    }
                    context.log.debug(record)
                    records.append(record)
                    zip_ref.extract(file_name, Path(CACHE_DIR) / source)

        # Create a dynamic partition for the datasets listed in the catalog
        catalog_df: pd.DataFrame = pd.DataFrame.from_records(records)
        catalog_df["partition_key"] = (
            catalog_df[["year", "resolution", "table_name"]]
            .astype(str)
            .agg(lambda s: "/".join(s).rsplit(".")[0], axis=1)
        )

        # TODO: add filter for prod vs. dev mode
        self.add_partition_keys(context, catalog_df["partition_key"].to_list())
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

    def _country_metadata(self, _context) -> CountryMetadata:
        return self.country_metadata

    def _data_publisher(
        self, _context, country_metdata: CountryMetadata
    ) -> DataPublisher:
        return DataPublisher(
            name="National Records of Scotland",
            url="https://www.nrscotland.gov.uk/",
            description="National Records of Scotland (NRS) is a Non-Ministerial Department of "
            "the Scottish Government. Our purpose is to collect, preserve and "
            "produce information about Scotland's people and history and make it "
            "available to inform current and future generations.",
            countries_of_interest=[country_metdata.id],
        )

    def create_lookup(self):
        @asset(key_prefix=self.key_prefix)
        def lookup(context):
            url = "https://www.nrscotland.gov.uk/files/geography/2011-census/geog-2011-cen-supp-info-oldoa-newoa-lookup.xls"
            df_oa_to_council = (
                pd.read_excel(url, sheet_name="2011OA_Lookup", storage_options=HEADERS)
                .iloc[:-2]
                .loc[:, ["OutputArea2011Code", "CouncilArea2011Code"]]
            )
            url = "https://www.nrscotland.gov.uk/files//geography/2011-census/OA_DZ_IZ_2011.xlsx"
            df_oa_to_dz_iz = pd.read_excel(
                url, sheet_name="OA_DZ_IZ_2011 Lookup", storage_options=HEADERS
            )
            df_dz_nm = pd.read_excel(
                url, sheet_name="DataZone2011Lookup", storage_options=HEADERS
            )
            df_iz_nm = pd.read_excel(
                url, sheet_name="IntermediateZone2011Lookup", storage_options=HEADERS
            )
            combined = (
                df_oa_to_council.merge(df_oa_to_dz_iz, on=["OutputArea2011Code"])
                .merge(df_dz_nm, on=["DataZone2011Code"])
                .merge(df_iz_nm, on=["IntermediateZone2011Code"])
            )
            combined["OutputArea2011Name"] = combined["OutputArea2011Code"].copy()
            df_council_name = pd.read_excel(
                "https://www.nrscotland.gov.uk/files//geography/2011-census/oa2011-to-hba2014.xls",
                sheet_name="HealthBoard2014_Council2011",
                storage_options=HEADERS,
            )
            combined = combined.merge(
                df_council_name[["CouncilArea2011Code", "NRSCouncilAreaName"]],
                on="CouncilArea2011Code",
            ).rename(columns={"NRSCouncilAreaName": "CouncilArea2011Name"})
            context.add_output_metadata(
                metadata={
                    "lookup_shape": f"{combined.shape[0]} rows x {combined.shape[1]} columns",
                    "lookup_preview": MetadataValue.md(combined.head().to_markdown()),
                },
            )
            return combined

        return lookup

    def create_geometry(self):
        """
        Creates an asset providing a list of geometries, metadata and names
        at different resolutions.
        """

        @send_to_geometry_sensor
        @asset(key_prefix=self.key_prefix)
        def geometry(context, lookup: pd.DataFrame) -> list[GeometryOutput]:
            """List of geometries, metadata and names at different resolutions."""
            geometries_to_return = []
            for level_details in SCOTLAND_GEO_LEVELS.values():
                # TODO: get correct values
                geometry_metadata = GeometryMetadata(
                    country_metadata=self.country_metadata,
                    validity_period_start=CENSUS_COLLECTION_DATE,
                    validity_period_end=CENSUS_COLLECTION_DATE,
                    level=level_details.level,
                    hxl_tag=level_details.hxl_tag,
                )
                file_name = download_file(CACHE_DIR, level_details.url)
                region_geometries_raw: gpd.GeoDataFrame = gpd.read_file(
                    f"zip://{file_name}"
                )
                context.log.debug(ic(region_geometries_raw.head()))
                context.log.debug(ic(region_geometries_raw.columns))
                context.log.debug(ic(lookup.columns))
                region_geometries_merge = region_geometries_raw.merge(
                    lookup,
                    left_on=level_details.left_on,
                    right_on=level_details.right_on,
                )

                region_geometries_merge = region_geometries_merge.dissolve(
                    by=level_details.geo_id_column
                ).reset_index()

                context.log.debug(ic(region_geometries_merge.head()))
                context.log.debug(ic(region_geometries_merge.columns))
                region_geometries = region_geometries_merge.rename(
                    columns={level_details.geo_id_column: COL.GEO_ID.value}
                ).loc[:, ["geometry", COL.GEO_ID.value]]

                region_names = (
                    region_geometries_merge.rename(
                        columns={
                            level_details.geo_id_column: COL.GEO_ID.value,
                        }
                        | {
                            value: key
                            for key, value in level_details.name_columns.items()
                        }
                    )
                    .loc[
                        :, [COL.GEO_ID.value, *list(level_details.name_columns.keys())]
                    ]
                    .drop_duplicates()
                )
                geometries_to_return.append(
                    GeometryOutput(
                        metadata=geometry_metadata,
                        gdf=region_geometries,
                        names_df=region_names,
                    )
                )

            # Add output metadata
            geo: GeometryOutput = geometries_to_return[0]
            first_metadata, first_gdf, first_names = geo.metadata, geo.gdf, geo.names_df
            first_joined_gdf = first_gdf.merge(first_names, on=COL.GEO_ID.value)
            ax = first_joined_gdf.plot(column="eng", legend=False)
            ax.set_title(f"Scotland 2011 {first_metadata.level}")
            md_plot = markdown_from_plot()
            context.add_output_metadata(
                metadata={
                    "all_geom_levels": MetadataValue.md(
                        ",".join([geo.metadata.level for geo in geometries_to_return])
                    ),
                    "first_geometry_plot": MetadataValue.md(md_plot),
                    "first_names_preview": MetadataValue.md(
                        first_names.head().to_markdown()
                    ),
                }
            )

            return geometries_to_return

        return geometry

    def _geometry(self, context):
        # Not required as geometry overridden
        pass

    def _source_data_releases(
        self,
        _context,
        geometry: list[GeometryOutput],
        data_publisher: DataPublisher,
        # TODO: consider version without inputs so only output type specified
        # **kwargs,
    ) -> dict[str, SourceDataRelease]:
        source_data_releases = {}
        for geo in geometry:
            for (
                source_data_release_id,
                source_data_release,
            ) in SOURCE_DATA_RELEASES.items():
                source_data_release_new: SourceDataRelease = SourceDataRelease(
                    name=source_data_release.name,
                    date_published=source_data_release.date_published,
                    reference_period_start=source_data_release.collection_period_start,
                    reference_period_end=source_data_release.reference_period_end,
                    collection_period_start=source_data_release.collection_period_start,
                    collection_period_end=source_data_release.collection_period_end,
                    expect_next_update=source_data_release.expect_next_update,
                    url=source_data_release.url,
                    data_publisher_id=data_publisher.id,
                    description=source_data_release.description,
                    geometry_metadata_id=geo.metadata.id,
                )
                combined_level_and_release_id = get_source_data_release(
                    geo.metadata.level, source_data_release_id
                )
                source_data_releases[
                    combined_level_and_release_id
                ] = source_data_release_new
        return source_data_releases

    @staticmethod
    def get_table(context, table_details) -> pd.DataFrame:
        table_df = pd.read_csv(Path(CACHE_DIR) / table_details["file_name"].iloc[0])
        add_metadata(context, table_df, table_details["partition_key"].iloc[0])
        return table_df

    def _census_tables(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        """Creates individual census tables as dataframe."""
        partition_key = context.asset_partition_key_for_output()
        context.log.info(partition_key)
        table_details = catalog.loc[catalog["partition_key"].isin([partition_key])]
        context.log.info(table_details)
        return self.get_table(context, table_details)

    @staticmethod
    def census_table_metadata(
        catalog_row: dict[str, str],
        source_table: SourceTable,
        source_data_releases: dict[str, SourceDataRelease],
    ) -> MetricMetadata:
        return MetricMetadata(
            human_readable_name=catalog_row["human_readable_name"],
            source_download_url=catalog_row["source_download_url"],
            source_archive_file_path=catalog_row["source_archive_file_path"],
            source_documentation_url=catalog_row["source_documentation_url"],
            source_data_release_id=source_data_releases[
                get_source_data_release(
                    source_table.geo_level, catalog_row["census_release"]
                )
            ].id,
            # TODO - this is a placeholder
            parent_metric_id="unknown_at_this_stage",
            potential_denominator_ids=None,
            parquet_margin_of_error_file=None,
            parquet_margin_of_error_column=None,
            parquet_column_name=source_table.source_column,
            # TODO - this is a placeholder
            metric_parquet_path="unknown_at_this_stage",
            hxl_tag=source_table.hxltag,
            description=catalog_row["description"],
            source_metric_id=source_table.hxltag,
        )

    def _source_metric_metadata(
        self,
        context,
        catalog: pd.DataFrame,
        source_data_releases: dict[str, SourceDataRelease],
    ) -> MetricMetadata:
        partition_key = context.partition_key
        catalog_row = catalog[catalog["partition_key"] == partition_key].to_dict(
            orient="records"
        )[0]

        geo_level = partition_key.split("/")[1]
        source_table = SourceTable(
            # TODO: how programmatically do this
            hxltag="TBD",
            geo_level=geo_level,
            geo_column=SCOTLAND_GEO_LEVELS[geo_level].geo_id_column,
            # TODO: update this
            source_column="Count",
        )

        return self.census_table_metadata(
            catalog_row,
            source_table,
            source_data_releases,
        )

    def _derived_metrics(
        self,
        context,
        census_tables: pd.DataFrame,
        source_metric_metadata: MetricMetadata,
    ) -> MetricsOutput:
        try:
            SEP = "__"
            partition_key = context.partition_key
            source_mmd = source_metric_metadata
            parquet_file_name = (
                f"{self.key_prefix}/metrics/"
                f"{''.join(c for c in partition_key if c.isalnum()) + '.parquet'}"
            )
            derived_metrics, derived_mmd = [], []

            # If derived metrics
            try:
                metric_specs = DERIVED_COLUMN_SPECIFICATIONS[partition_key]

                def reshape(df_to_reshape: pd.DataFrame) -> pd.DataFrame:
                    df_to_reshape = df_to_reshape.rename(
                        columns={
                            "Unnamed: 0": COL.GEO_ID.value,
                            "Unnamed: 1": "Age Category",
                        }
                    ).drop(columns=["All people"])
                    df_to_reshape = df_to_reshape.melt(
                        [COL.GEO_ID.value, "Age Category"],
                        var_name="Sex Label",
                        value_name="Count",
                    )
                    df_to_reshape["Sex Label"] = df_to_reshape["Sex Label"].map(
                        {"Males": "Male", "Females": "Female"}
                    )
                    return df_to_reshape

                census_tables_for_derived_metrics = reshape(census_tables)
                source_column = source_mmd.parquet_column_name
                for metric_spec in metric_specs:
                    new_table = (
                        census_tables_for_derived_metrics.pipe(metric_spec.filter_func)
                        .groupby(by=COL.GEO_ID.value, as_index=True)
                        .sum()
                        .rename(columns={source_column: metric_spec.output_column_name})
                        .filter(
                            items=[COL.GEO_ID.value, metric_spec.output_column_name]
                        )
                    )
                    derived_metrics.append(new_table)
                    new_mmd = source_mmd.copy()
                    new_mmd.parent_metric_id = source_mmd.source_metric_id
                    new_mmd.metric_parquet_path = parquet_file_name
                    new_mmd.hxl_tag = metric_spec.hxltag
                    new_mmd.parquet_column_name = metric_spec.output_column_name
                    new_mmd.human_readable_name = metric_spec.human_readable_name
                    derived_mmd.append(new_mmd)
            except KeyError:
                # No extra derived metrics specified for this partition -- only use
                # those from pivoted data
                pass

            # Batch
            def make_pivot(df: pd.DataFrame) -> pd.DataFrame:
                # TODO: reshape based on Unnamed: 1 to Unnamed N
                pivot_cols = [
                    col
                    for col in df.columns
                    if col != "Unnamed: 0" and col.startswith("Unnamed: ")
                ]
                pivot = df.pivot_table(
                    index="Unnamed: 0", columns=pivot_cols, aggfunc="sum"
                )

                # FLattent multi-index
                if isinstance(pivot.columns, pd.MultiIndex):
                    pivot.columns = [
                        SEP.join(list(map(str, col))).strip()
                        for col in pivot.columns.to_numpy()
                    ]
                # Ensure columns are string
                else:
                    pivot.columns = [
                        str(col).strip() for col in pivot.columns.to_numpy()
                    ]

                pivot.index = pivot.index.rename(COL.GEO_ID.value)

                return pivot

            new_table = make_pivot(census_tables)

            # Split for description of metrics
            exceptions = {
                "Age by single year": ["Age by single year"],
                "National Statistics Socio-economic Classification (NS-SeC) by ethnic group by sex by age": [
                    "Ethnic group",
                    "Sex and Age",
                    "National Statistics Socio-economic Classification (NS-SeC)",
                ],
                # 2011/CouncilArea2011/DC1104SC
                "Residence type by sex by age": ["Residence type and Sex", "Age"],
                # 2011/CouncilArea2011/DC1106SC
                "Schoolchildren and full-time students living away from home during term time by sex by age": [
                    "Schoolchildren and full-time students living away from home during term time and Sex",
                    "Age",
                ],
                # 2011/CouncilArea2011/DC1112SC
                "Dependent children by household type by sex by age": [
                    "Dependent children by household type",
                    "Sex",
                    "Age",
                ],
            }
            if source_mmd.description not in exceptions:
                split = source_mmd.description.split(" by ")[::-1]
            else:
                split = exceptions[source_mmd.description]
            out_cols = [
                "".join(x for x in col.title() if not x.isspace()) for col in split
            ]
            context.log.debug(ic(out_cols))
            ic(new_table.columns)

            for metric_col in new_table.columns:
                metric_df = new_table.loc[:, metric_col].to_frame()
                ic(metric_df)
                derived_metrics.append(metric_df)
                new_mmd = source_mmd.copy()
                new_mmd.parent_metric_id = source_mmd.source_metric_id
                new_mmd.metric_parquet_path = parquet_file_name

                # TODO: fix automating the hxltag
                key_val = dict(zip(out_cols, metric_col.split(SEP), strict=True))

                def gen_hxltag(kv: dict[str, str]) -> str:
                    out = ["#population"]
                    for key, value in kv.items():
                        out += [
                            "".join(c for c in key if c.isalnum())
                            + "_"
                            + "".join(c for c in value if c.isalnum())
                        ]
                    return "+".join(out)

                new_mmd.hxl_tag = gen_hxltag(key_val)
                new_mmd.parquet_column_name = metric_col
                context.log.debug(ic(key_val))
                # TODO: Update after fixing hxltag
                new_mmd.human_readable_name = "; ".join(
                    [
                        f"Variable: '{key}'; Value: '{value}'"
                        for key, value in key_val.items()
                    ]
                )
                derived_mmd.append(new_mmd)

            joined_metrics = reduce(
                lambda left, right: left.merge(
                    right, on=COL.GEO_ID.value, how="inner", validate="one_to_one"
                ),
                derived_metrics,
            )

            def make_int(maybe_non_int_df: pd.DataFrame) -> pd.DataFrame:
                for col in maybe_non_int_df:
                    if maybe_non_int_df[col].dtype == "object":
                        maybe_non_int_df[col] = (
                            maybe_non_int_df[col]
                            .str.replace(",", "")
                            .str.replace("-", "0")
                            .fillna("0")
                            .astype(int)
                        )
                return maybe_non_int_df

            # Fix format
            joined_metrics = make_int(joined_metrics)

            # Filter out whole country Scotland
            joined_metrics = joined_metrics.loc[
                ~joined_metrics.index.isin(["S92000003"])
            ]

            context.add_output_metadata(
                metadata={
                    "metadata_preview": MetadataValue.md(
                        metadata_to_dataframe(derived_mmd).head().to_markdown()
                    ),
                    "metrics_shape": f"{joined_metrics.shape[0]} rows x {joined_metrics.shape[1]} columns",
                    "metrics_preview": MetadataValue.md(
                        joined_metrics.head().to_markdown()
                    ),
                },
            )

        except Exception as err:
            err_msg = (
                f"Failed to automatically derive metrics for '{partition_key}' with "
                f"error: {err}"
            )
            context.log.error(err_msg)
            if self.allow_missing_derived_metrics:
                return MetricsOutput(metadata=[], metrics=pd.DataFrame())

        return MetricsOutput(metadata=derived_mmd, metrics=joined_metrics)


# Create assets
scotland = Scotland()
country_metadata = scotland.create_country_metadata()
data_publisher = scotland.create_data_publisher()
lookup = scotland.create_lookup()
geometry = scotland.create_geometry()
source_data_releases = scotland.create_source_data_releases()
catalog = scotland.create_catalog()
census_tables = scotland.create_census_tables()
source_metric_metadata = scotland.create_source_metric_metadata()
derived_metrics = scotland.create_derived_metrics()
# Publish all partitions
metrics = scotland.create_metrics(None)
