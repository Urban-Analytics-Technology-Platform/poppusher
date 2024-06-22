#!/usr/bin/python3
from __future__ import annotations

import urllib.parse as urlparse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from functools import reduce
from pathlib import Path
from typing import ClassVar

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
import zipfile_deflate64 as zipfile
from dagster import (
    MetadataValue,
    asset,
)
from icecream import ic

from popgetter.assets.country import Country
from popgetter.cloud_outputs import send_to_geometry_sensor
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
    metadata_to_dataframe,
)
from popgetter.utils import add_metadata, markdown_from_plot

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
# Currently including only releases matching tables included
REQUIRED_RELEASES = ["3A", "3I", "2A", "3C"]
GENERAL_METHODS_URL = "https://www.scotlandscensus.gov.uk/media/jx2lz54n/scotland-s_census_2011_general_report.pdf"
CENSUS_REFERENCE_DATE = date(2011, 3, 27)
CENSUS_COLLECTION_DATE = date(2011, 3, 27)
CENSUS_EXPECT_NEXT_UPDATE = date(2022, 1, 1)

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
        description="TBC",
        # geography_file="TBC",
        # geography_level="TBC",
        geometry_metadata_id="TBC",
        # countries_of_interest=[country.id],
    ),
    "3I": SourceDataRelease(
        name="Census 2011: Release 3I",
        date_published=date(2014, 9, 24),
        reference_period_start=date(2015, 10, 22),
        reference_period_end=date(2015, 10, 22),
        collection_period_start=date(2011, 10, 22),
        collection_period_end=date(2011, 10, 22),
        expect_next_update=date(2022, 1, 1),
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3i",
        data_publisher_id="TBD",
        description="TBC",
        # geography_file="TBC",
        # geography_level="TBC",
        geometry_metadata_id="TBC",
        # countries_of_interest=[country.id],
    ),
    "2A": SourceDataRelease(
        name="Census 2011: Release 2A",
        date_published=date(2013, 9, 26),
        reference_period_start=date(2015, 10, 22),
        reference_period_end=date(2015, 10, 22),
        collection_period_start=date(2011, 10, 22),
        collection_period_end=date(2011, 10, 22),
        expect_next_update=date(2022, 1, 1),
        url="https://www.nrscotland.gov.uk/news/2013/census-2011-release-2a",
        data_publisher_id="TBD",
        description="TBC",
        # geography_file="TBC",
        # geography_level="TBC",
        geometry_metadata_id="",
        # countries_of_interest=[country.id],
    ),
    "3C": SourceDataRelease(
        name="Census 2011: Release 3C",
        date_published=date(2014, 4, 9),
        reference_period_start=date(2015, 10, 22),
        reference_period_end=date(2015, 10, 22),
        collection_period_start=date(2011, 10, 22),
        collection_period_end=date(2011, 10, 22),
        expect_next_update=date(2022, 1, 1),
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-releases-2d-and-3c",
        data_publisher_id="TBD",
        description="TBC",
        geometry_metadata_id="",
        # geography_file="TBC",
        # geography_level="TBC",
        # countries_of_interest=[country.id],
    ),
}


# Move to tests
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
        name_columns={"en": "OutputArea2011Name"},  # TODO
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
        name_columns={"en": "Name"},
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
    #     name_columns={"en": "OA_CODE"},
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
        name_columns={"en": "CouncilArea2011Name"},
        url="https://maps.gov.scot/ATOM/shapefiles/SG_DataZoneBdry_2011.zip",
        lookup_url=None,
        lookup_sheet=None,
        left_on="DataZone",
        right_on="DataZone2011Code",
    ),
}


# cache_dir = tempfile.mkdtemp()
cache_dir = "./cache"


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
age_code = "`Age Code`"
sex_label = "`Sex Label`"
DERIVED_COLUMNS = [
    DerivedColumn(
        hxltag="#population+children+age5_17",
        filter_func=lambda df: df.query(f"{age_code} >= 5 and {age_code} < 18"),
        output_column_name="children_5_17",
        human_readable_name="Children aged 5 to 17",
    ),
    DerivedColumn(
        hxltag="#population+infants+age0_4",
        filter_func=lambda df: df.query(f"{age_code} >= 0 and {age_code} < 5"),
        output_column_name="infants_0_4",
        human_readable_name="Infants aged 0 to 4",
    ),
    DerivedColumn(
        hxltag="#population+children+age0_17",
        filter_func=lambda df: df.query(f"{age_code} >= 0 and {age_code} < 18"),
        output_column_name="children_0_17",
        human_readable_name="Children aged 0 to 17",
    ),
    DerivedColumn(
        hxltag="#population+adults+f",
        filter_func=lambda df: df.query(
            f"{age_code} >= 18 and {sex_label} == 'Female'"
        ),
        output_column_name="adults_f",
        human_readable_name="Female adults",
    ),
    DerivedColumn(
        hxltag="#population+adults+m",
        filter_func=lambda df: df.query(f"{age_code} >= 18 and {sex_label} == 'Male'"),
        output_column_name="adults_m",
        human_readable_name="Male adults",
    ),
    DerivedColumn(
        hxltag="#population+adults",
        filter_func=lambda df: df.query(f"{age_code} >= 18"),
        output_column_name="adults",
        human_readable_name="Adults",
    ),
    DerivedColumn(
        hxltag="#population+ind",
        filter_func=lambda df: df,
        output_column_name="individuals",
        human_readable_name="Total individuals",
    ),
]

TABLES_TO_PROCESS: list[str] = [
    "QS103SC",
    "QS104SC",
    "KS201SC",
    "DC1117SC",
    "DC2101SC",
    "DC6206SC",
    "LC1117SC",
]

PARTITIONS_TO_PUBLISH: list[str] = ["2011/OutputArea2011/LC1117SC"]


DERIVED_COLUMN_SPECIFICATIONS: dict[str, list[DerivedColumn]] = {
    PARTITIONS_TO_PUBLISH[0]: DERIVED_COLUMNS,
}


def get_source_data_release(geo_level: str, cenesus_release: str) -> str:
    return geo_level + "_" + cenesus_release


class Scotland(Country):
    key_prefix: str = "scotland"
    geo_levels: ClassVar[list[str]] = list(SCOTLAND_GEO_LEVELS.keys())
    tables_to_process: list[str] | None = TABLES_TO_PROCESS

    def _catalog(self, context) -> pd.DataFrame:
        """Creates a catalog of the individual census tables from all data sources."""

        def source_to_zip(source_name: str, url: str) -> Path:
            """Downloads if necessary and returns the name of the locally cached zip file
            of the source data (replacing spaces with _)"""
            file_name = Path(cache_dir) / (source_name.replace(" ", "_") + ".zip")
            return download_file(cache_dir, url, file_name)

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
                    zip_ref.extract(file_name, Path(cache_dir) / source)

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
        return CountryMetadata(
            name_short_en="Scotland",
            name_official="Scotland",
            iso3="GBR",
            iso2="GB",
            iso3166_2="GB-SCT",
        )

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
        def geometry(
            context, lookup: pd.DataFrame
        ) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
            """List of geometries, metadata and names at different resolutions."""
            geometries_to_return = []
            for level_details in SCOTLAND_GEO_LEVELS.values():
                # TODO: get correct values
                geometry_metadata = GeometryMetadata(
                    validity_period_start=CENSUS_COLLECTION_DATE,
                    validity_period_end=CENSUS_COLLECTION_DATE,
                    level=level_details.level,
                    hxl_tag=level_details.hxl_tag,
                )
                file_name = download_file(cache_dir, level_details.url)
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
                    columns={level_details.geo_id_column: "GEO_ID"}
                ).loc[:, ["geometry", "GEO_ID"]]

                region_names = (
                    region_geometries_merge.rename(
                        columns={
                            level_details.geo_id_column: "GEO_ID",
                        }
                        | {
                            value: key
                            for key, value in level_details.name_columns.items()
                        }
                    )
                    .loc[:, ["GEO_ID", *list(level_details.name_columns.keys())]]
                    .drop_duplicates()
                )
                geometries_to_return.append(
                    (geometry_metadata, region_geometries, region_names)
                )

            # Add output metadata
            first_metadata, first_gdf, first_names = geometries_to_return[0]
            first_joined_gdf = first_gdf.merge(first_names, on="GEO_ID")
            ax = first_joined_gdf.plot(column="en", legend=False)
            ax.set_title(f"Scotland 2011 {first_metadata.level}")
            md_plot = markdown_from_plot(plt)
            context.add_output_metadata(
                metadata={
                    "all_geom_levels": MetadataValue.md(
                        ",".join(
                            [metadata.level for metadata, _, _ in geometries_to_return]
                        )
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
        geometry: list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]],
        data_publisher: DataPublisher,
        # TODO: consider version without inputs so only output type specified
        # **kwargs,
    ) -> dict[str, SourceDataRelease]:
        source_data_releases = {}
        for geo_metadata, _, _ in geometry:
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
                    geometry_metadata_id=geo_metadata.id,
                )
                combined_level_and_release_id = get_source_data_release(
                    geo_metadata.level, source_data_release_id
                )
                source_data_releases[
                    combined_level_and_release_id
                ] = source_data_release_new
        return source_data_releases

    @staticmethod
    def get_table(context, table_details) -> pd.DataFrame:
        table_df = pd.read_csv(Path(cache_dir) / table_details["file_name"].iloc[0])
        add_metadata(context, table_df, table_details["partition_key"].iloc[0])
        return table_df

    def _census_tables(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        """Creates individual census tables as dataframe."""
        partition_key = context.asset_partition_key_for_output()
        context.log.info(partition_key)
        table_details = catalog.loc[catalog["partition_key"].isin([partition_key])]
        context.log.info(table_details)
        return self.get_table(context, table_details)

    # subset_partition_keys: list[str] = ["2011/OA11/LC1117SC"]
    # subset_mapping = SpecificPartitionsPartitionMapping(subset_partition_keys)
    # subset_partition = StaticPartitionsDefinition(subset_partition_keys)

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
    ) -> tuple[list[MetricMetadata], pd.DataFrame]:
        ...
        SEP = "__"
        partition_key = context.partition_key
        source_mmd = source_metric_metadata
        parquet_file_name = (
            "".join(c for c in partition_key if c.isalnum()) + ".parquet"
        )
        derived_metrics, derived_mmd = [], []

        # If derived metrics
        # try:
        #     metric_specs = DERIVED_COLUMN_SPECIFICATIONS[partition_key]
        #     source_column = source_mmd.parquet_column_name
        #     for metric_spec in metric_specs:
        #         new_table = (
        #             census_tables.pipe(metric_spec.filter_func)
        #             .groupby(by="GEO_ID", as_index=True)
        #             .sum()
        #             .rename(columns={source_column: metric_spec.output_column_name})
        #             .filter(items=["GEO_ID", metric_spec.output_column_name])
        #         )
        #         derived_metrics.append(new_table)
        #         new_mmd = source_mmd.copy()
        #         new_mmd.parent_metric_id = source_mmd.source_metric_id
        #         new_mmd.metric_parquet_path = parquet_file_name
        #         new_mmd.hxl_tag = metric_spec.hxltag
        #         new_mmd.parquet_column_name = metric_spec.output_column_name
        #         new_mmd.human_readable_name = metric_spec.human_readable_name
        #         derived_mmd.append(new_mmd)
        # except KeyError:
        #     # No extra derived metrics specified for this partition -- only use
        #     # those from pivoted data
        #     pass

        # Batch
        def make_pivot(df: pd.DataFrame) -> pd.DataFrame:
            # TODO: reshape based on Unnamed: 1 to Unnamed N
            pivot_cols = [
                col
                for col in census_tables.columns
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
                pivot.columns = [str(col).strip() for col in pivot.columns.to_numpy()]

            pivot.index = pivot.index.rename("GEO_ID")

            return pivot

        new_table = make_pivot(census_tables)
        out_cols = [
            "".join(x for x in col.title() if not x.isspace())
            for col in source_mmd.description.split(" by ")[::-1]
        ]

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
                right, on="GEO_ID", how="inner", validate="one_to_one"
            ),
            derived_metrics,
        )

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
        return derived_mmd, joined_metrics


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
metrics = scotland.create_metrics()