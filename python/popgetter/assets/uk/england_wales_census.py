from __future__ import annotations

from dataclasses import dataclass
import os
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, ClassVar
from urllib.parse import urljoin
from collections.abc import Callable

from dagster import MetadataValue
from matplotlib import pyplot as plt
import pandas as pd
import geopandas as gpd
import requests
from bs4 import BeautifulSoup
from icecream import ic
from pandas import DataFrame

import popgetter
from popgetter.assets.country import Country
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
)
from popgetter.utils import (
    SourceDataAssumptionsOutdated,
    add_metadata,
    extract_main_file_from_zip,
    markdown_from_plot,
)

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


@dataclass
class EWCensusGeometryLevel:
    level: str
    geo_id_column: str
    census_table_column: str | None
    name_columns: dict[str, str]  # keys = language codes, values = column names
    data_download_url: str
    documentation_url: str
    hxl_tag: str = ""

    def __post_init__(self):
        if self.hxl_tag == "":
            self.hxl_tag=f"#geo+bounds+code+{self.level}"

@dataclass
class SourceTable:
    hxltag: str
    geo_level: str
    geo_column: str
    source_column: str

@dataclass
class DerivedColumn:
    hxltag: str
    filter_func: Callable[[pd.DataFrame], pd.DataFrame]
    output_column_name: str
    human_readable_name: str



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
        source_data_release_id=source_data_releases[source_table.geo_level].id,
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


CENSUS_COLLECTION_DATE = date(2021, 3, 21)

EW_CENSUS_GEO_LEVELS : dict[str, EWCensusGeometryLevel] = {
    "oa": EWCensusGeometryLevel(
        level="oa",
        geo_id_column="oa21cd",
        census_table_column=None,
        name_columns= {"en": "name"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_oa_2021.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_oa_2021",
    ),
    "lsoa": EWCensusGeometryLevel(
        level="lsoa",
        geo_id_column="lsoa21cd",
        census_table_column=None,
        name_columns= {"en": "name"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_lsoa_2021.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_lsoa_2021",
    ),
    "msoa": EWCensusGeometryLevel(
        level="msoa",
        geo_id_column="msoa21cd",
        census_table_column=None,
        name_columns= {"en": "name"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_msoa_2021.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_msoa_2021",
    ),
    "ltla": EWCensusGeometryLevel(
        level="ltla",
        geo_id_column="ltla22cd",
        census_table_column=None,
        name_columns= {"en": "ltla22nm", "cy": "ltla22nmw"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_ltla_2022.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_ltla_2022",
    ),
    "rgn": EWCensusGeometryLevel(
        level="rgn",
        geo_id_column="rgn22cd",
        census_table_column=None,
        name_columns= {"en": "rgn22nm", "cy": "rgn22nmw"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_rgn_2022.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_rgn_2022",
    ),
    "ctry": EWCensusGeometryLevel(
        level="ctry",
        geo_id_column="ctry22cd",
        census_table_column=None,
        name_columns= {"en": "ctry22nm", "cy": "ctry22nmw"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_ctry_2022.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_ctry_2022",
    ),
}


# TODO - this is probably only required for tests,
# hence would be best move to a test fixture
REQUIRED_TABLES = ["TS009"] if os.getenv("ENV") == "dev" else None


age_code = "`Age Code`"
sex_label = "`Sex Label`"
DERIVED_COLUMNS = [
    DerivedColumn(
        hxltag="#population+children+age5_17",
        # FIXME - this lambda function is not correct
        filter_func=lambda df: df.query(f"{age_code} >= 5 and {age_code} < 18"),
        output_column_name="children_5_17",
        human_readable_name="Children aged 5 to 17",
    ),
]

# Lookup of `partition_key` (eg geom + source table id) to `DerivedColumn` (columns that can be derived from the source table) 
DERIVED_COLUMN_SPECIFICATIONS: dict[str, list[DerivedColumn]] = {
    "ltla/TS009": DERIVED_COLUMNS,
}

class EnglandAndWales(Country):
    key_prefix: ClassVar[str] = "england_wales"
    geo_levels: ClassVar[list[str]] = list(EW_CENSUS_GEO_LEVELS.keys())
    required_tables: list[str] | None = REQUIRED_TABLES

    def _country_metadata(self, _context) -> CountryMetadata:
        return country

    def _data_publisher(
        self, _context, _country_metdata: CountryMetadata
    ) -> DataPublisher:
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

        bulk_downloads_df = bulk_downloads_webpage()

        for bulk_downloads_index, row in bulk_downloads_df.iterrows():
            table_id = row["table_id"]

            source_documentation_url = _guess_source_documentation_url(table_id)

            # Get description of the table
            # TODO - For now this is page scraping the description from the source_documentation_url page
            # In the future we should retrieve the description by finding a suitable API call.
            # The relevant API is here "https://www.nomisweb.co.uk/api/v01/"
            description = row["table_name"]
            description = _retrieve_table_description(source_documentation_url)

            # For now this does not use the "extra_post_release_filename" and "extra_post_release_url" tables
            for geo_level in self.geo_levels:
                # get the path within the zip file
                archive_file_path = _guess_csv_filename(
                    row["original_release_filename"], geo_level
                )

                catalog_summary["node"].append(bulk_downloads_index)
                catalog_summary["table_id"].append(table_id)
                catalog_summary["geo_level"].append(geo_level)
                catalog_summary["partition_key"].append(f"{geo_level}/{table_id}")
                catalog_summary["human_readable_name"].append(row["table_name"])
                # TODO - For now this is the same as the human readable name
                # In the future we should retrieve the description by scraping the page or finding a suitable API call.
                catalog_summary["description"].append(description)
                catalog_summary["metric_parquet_file_url"].append(None)
                catalog_summary["parquet_column_name"].append(None)
                catalog_summary["parquet_margin_of_error_column"].append(None)
                catalog_summary["parquet_margin_of_error_file"].append(None)
                catalog_summary["potential_denominator_ids"].append(None)
                catalog_summary["parent_metric_id"].append(None)
                catalog_summary["source_data_release_id"].append(None)
                catalog_summary["source_download_url"].append(
                    row["original_release_url"]
                )
                catalog_summary["source_format"].append(None)
                catalog_summary["source_archive_file_path"].append(archive_file_path)
                catalog_summary["source_documentation_url"].append(
                    source_documentation_url
                )

        catalog_df = pd.DataFrame.from_records(catalog_summary)
        self.add_partition_keys(context, catalog_df["partition_key"].to_list())

        add_metadata(context, catalog_df, "Catalog")
        return catalog_df

    def _census_tables(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        """
        WIP:
        For now this function:
        - downloads all of the "main" zip files
        - extracts all of the CSV files from each zip file
        - reads the CSV files into a DataFrame
        - lists the columns of the DataFrame
        """
        partition_key = context.asset_partition_key_for_output()
        current_table = catalog[catalog["partition_key"] == partition_key]

        source_download_url = current_table["source_download_url"].values[0]
        source_archive_file_path = current_table["source_archive_file_path"].values[0]

        del_temp_dir = False if os.getenv("ENV") == "dev" else True
        with TemporaryDirectory(delete=del_temp_dir) as temp_dir:
            ic(source_download_url)
            temp_zip = Path(_download_zipfile(source_download_url, temp_dir))

            # This is a workaround for the fact that there is an upstream bug that results
            # in some of the filenames have two consecutive `.` in the filename
            # e.g. `census2021-ts002-lsoa..csv`
            extract_file_path = None
            try:
                extract_file_path = extract_main_file_from_zip(
                    temp_zip, Path(temp_dir), source_archive_file_path
                )
            except ValueError:
                source_archive_file_path = str(
                    Path(source_archive_file_path).with_suffix("..csv")
                )
                extract_file_path = extract_main_file_from_zip(
                    temp_zip, Path(temp_dir), source_archive_file_path
                )

            # If we still can't find the file, then there is a different, unforeseen problem
            if not extract_file_path:
                err_msg = f"Unable to find the file `{source_archive_file_path}` in the zip file: {source_download_url}"
                raise SourceDataAssumptionsOutdated(err_msg)

            census_table = pd.read_csv(extract_file_path)

        add_metadata(context, census_table, title=partition_key)
        return census_table

    def _derived_metrics(
        self, context, census_tables: DataFrame, source_metric_metadata: MetricMetadata
    ) -> tuple[list[MetricMetadata], DataFrame]:
        SEP = "_"
        partition_key = context.partition_key
        geo_level = partition_key.split("/")[0]
        source_table = census_tables
        source_mmd = source_metric_metadata
        source_column = source_mmd.parquet_column_name
        context.log.debug(ic(source_table.columns))
        context.log.debug(ic(source_column))
        context.log.debug(ic(source_table.head()))
        context.log.debug(ic(len(source_table)))

        # FIXME
        raise NotImplementedError("This function is not yet implemented")

    def _metrics(self, context, catalog: pd.DataFrame,
    ) -> list[tuple[str, list[MetricMetadata], pd.DataFrame]]:
        """
        This asset exists solely to aggregate all the derived tables into one
        single unpartitioned asset, which the downstream publishing tasks can use.
        """
        # Get derived_metrics asset for partitions that were successful
        derived_metrics_dict = {}
        for partition_key in catalog["partition_key"].to_list():
            try:
                derived_metrics_partition = popgetter.defs.load_asset_value(
                    [self.key_prefix, "derived_metrics"], partition_key=partition_key
                )
                derived_metrics_dict[partition_key] = derived_metrics_partition
            except FileNotFoundError as err:
                context.log.debug(ic(f"Failed partition key {partition_key}: {err}"))

        # Combine outputs across partitions
        outputs = [
            (mmds[0].metric_parquet_path, mmds, table)
            for (mmds, table) in derived_metrics_dict.values()
        ]
        context.add_output_metadata(
            metadata={
                "num_metrics": sum(len(output[1]) for output in outputs),
                "num_parquets": len(outputs),
            },
        )
        return outputs

    def _source_data_releases(
        self,
        _context,
        geometry: list[tuple[GeometryMetadata, Any, DataFrame]],
        data_publisher: DataPublisher,
    ) -> dict[str, SourceDataRelease]:
        source_data_releases = {}

        for geo_metadata, _, _ in geometry:
            source_data_release: SourceDataRelease = SourceDataRelease(
                name="Census 2021",
                date_published=date(2022, 6, 28),
                reference_period_start=CENSUS_COLLECTION_DATE,
                reference_period_end=CENSUS_COLLECTION_DATE,
                collection_period_start=CENSUS_COLLECTION_DATE,
                collection_period_end=CENSUS_COLLECTION_DATE,
                expect_next_update=date(2031, 1, 1),
                url="https://www.ons.gov.uk/census",
                data_publisher_id=data_publisher.id,
                # Taken from https://www.ons.gov.uk/census
                description="The census takes place every 10 years. It gives us a picture of all the people and households in England and Wales.",
                geometry_metadata_id=geo_metadata.id,
            )
            source_data_releases[geo_metadata.level] = source_data_release
        return source_data_releases

    def _source_metric_metadata(
        self,
        context,
        catalog: pd.DataFrame,
        source_data_releases: dict[str, SourceDataRelease],
    ) -> MetricMetadata:
        partition_key = context.partition_key
        if (
            self.required_tables is not None
            and partition_key not in DERIVED_COLUMN_SPECIFICATIONS
        ):
            skip_reason = (
                f"Skipping as requested partition {partition_key} is not configured "
                f"for derived metrics {DERIVED_COLUMN_SPECIFICATIONS.keys()}"
            )
            context.log.warning(skip_reason)
            raise RuntimeError(skip_reason)

        catalog_row = catalog[catalog["partition_key"] == partition_key].to_dict(
            orient="records"
        )[0]

        geo_level = catalog_row["geo_level"]
        source_table = SourceTable(
            # TODO: how programmatically do this
            hxltag="TBD",
            geo_level=geo_level,
            geo_column=EW_CENSUS_GEO_LEVELS[geo_level].geo_id_column,
            source_column="Count",
        )

        return census_table_metadata(
            catalog_row,
            source_table,
            source_data_releases,
        )

    def _geometry(self, context) -> list[tuple[GeometryMetadata, Any, DataFrame]]:
        # TODO: This is almost identical to Northern Ireland and Belgium so can probably be refactored to common
        # function with config of releases and languages
        geometries_to_return = []
        for level_details in EW_CENSUS_GEO_LEVELS.values():
            # TODO: get correct values
            geometry_metadata = GeometryMetadata(
                validity_period_start=CENSUS_COLLECTION_DATE,
                validity_period_end=CENSUS_COLLECTION_DATE,
                level=level_details.level,
                hxl_tag=level_details.hxl_tag,
            )
            geometries_raw: gpd.GeoDataFrame = gpd.read_file(level_details.data_download_url)

            context.log.debug(ic(level_details))
            context.log.debug(ic(geometries_raw.head(1).T))

            # Standardised the column names
            geometries_gdf = geometries_raw.rename(
                columns={level_details.geo_id_column: "GEO_ID"}
            ).loc[:, ["geometry", "GEO_ID"]]
            name_lookup_df = (
                geometries_raw.rename(
                    columns={
                        level_details.geo_id_column: "GEO_ID",
                        level_details.name_columns["en"]: "en",
                    }
                )
                .loc[:, ["GEO_ID", "en"]]
                .drop_duplicates()
            )
            geometries_to_return.append(
                (geometry_metadata, geometries_gdf, name_lookup_df)
            )

        # Add output metadata
        # TODO, It is not clear that this is the best way to represent the metadata
        first_metadata, first_gdf, first_names = geometries_to_return[0]
        first_joined_gdf = first_gdf.merge(first_names, on="GEO_ID")
        ax = first_joined_gdf.plot(column="en", legend=False)
        ax.set_title(f"England & Wales 2021 {first_metadata.level}")
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


def _guess_source_documentation_url(table_id):
    return f"https://www.nomisweb.co.uk/datasets/c2021{table_id.lower()}"


def _retrieve_table_description(source_documentation_url):
    soup = BeautifulSoup(
        requests.get(source_documentation_url).content, features="lxml"
    )
    landing_info = soup.find_all(id="dataset-landing-information")

    try:
        assert len(landing_info) == 1
        landing_info = landing_info[0]
    except AssertionError as ae:
        err_msg = f"Expected a single section with `id=dataset-landing-information`, but found {len(landing_info)}."
        raise SourceDataAssumptionsOutdated(err_msg) from ae

    return "\n".join([text.strip() for text in landing_info.stripped_strings])


def _guess_csv_filename(zip_filename, geometry_level):
    """
    Guess the name of the main file in the zip file.
    """
    stem = Path(zip_filename).stem
    return f"{stem}-{geometry_level}.csv"


def bulk_downloads_webpage() -> pd.DataFrame:
    """
    Get the list of bulk zip files from the bulk downloads page.
    """
    bulk_downloads_page = "https://www.nomisweb.co.uk/sources/census_2021_bulk"
    columns = ["table_id", "description", "original_release", "extra_post_release"]
    dfs = pd.read_html(bulk_downloads_page, header=0, extract_links="all")

    if len(dfs) != 1:
        err_msg = f"Expected a single table on the bulk downloads page, but found {len(dfs)} tables."
        raise SourceDataAssumptionsOutdated(err_msg)

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
