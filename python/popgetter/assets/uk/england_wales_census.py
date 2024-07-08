from __future__ import annotations

import os
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, ClassVar, Literal
from urllib.parse import urljoin

import geopandas as gpd
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dagster import MetadataValue
from icecream import ic
from pandas import DataFrame

import popgetter
from popgetter.assets.country import Country
from popgetter.cloud_outputs import GeometryOutput, MetricsOutput
from popgetter.metadata import (
    COL,
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
    metadata_to_dataframe,
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
    # TODO - I don't think this does anything meaningful
    # census_table_column: str | None
    name_columns: dict[str, str]  # keys = language codes, values = column names
    data_download_url: str
    documentation_url: str
    hxl_tag: str = ""

    def __post_init__(self):
        if self.hxl_tag == "":
            self.hxl_tag = f"#geo+bounds+code+{self.level}"


@dataclass
class SourceTable:
    hxltag: str
    geo_level: str
    geo_column: str
    source_column: str


@dataclass
class DerivedColumn:
    hxltag: str
    # If `None`, then just the named `source_column` will be used
    column_select: Callable[[pd.DataFrame], list[str]]
    # filter_func: Callable[[pd.DataFrame], pd.DataFrame]
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

EW_CENSUS_GEO_LEVELS: dict[str, EWCensusGeometryLevel] = {
    "oa": EWCensusGeometryLevel(
        level="oa",
        geo_id_column="oa21cd",
        # census_table_column=None,
        name_columns={"en": "name"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_oa_2021.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_oa_2021",
    ),
    "lsoa": EWCensusGeometryLevel(
        level="lsoa",
        geo_id_column="lsoa21cd",
        # census_table_column=None,
        name_columns={"en": "name"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_lsoa_2021.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_lsoa_2021",
    ),
    "msoa": EWCensusGeometryLevel(
        level="msoa",
        geo_id_column="msoa21cd",
        # census_table_column=None,
        name_columns={"en": "name"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_msoa_2021.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_msoa_2021",
    ),
    "ltla": EWCensusGeometryLevel(
        level="ltla",
        geo_id_column="ltla22cd",
        # census_table_column=None,
        name_columns={"en": "ltla22nm", "cy": "ltla22nmw"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_ltla_2022.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_ltla_2022",
    ),
    "rgn": EWCensusGeometryLevel(
        level="rgn",
        geo_id_column="rgn22cd",
        # census_table_column=None,
        name_columns={"en": "rgn22nm", "cy": "rgn22nmw"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_rgn_2022.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_rgn_2022",
    ),
    "ctry": EWCensusGeometryLevel(
        level="ctry",
        geo_id_column="ctry22cd",
        # census_table_column=None,
        name_columns={"en": "ctry22nm", "cy": "ctry22nmw"},
        data_download_url="https://borders.ukdataservice.ac.uk/ukborders/easy_download/prebuilt/shape/Ew_ctry_2022.zip",
        documentation_url="https://borders.ukdataservice.ac.uk/easy_download_data.html?data=Ew_ctry_2022",
    ),
}


# TODO - this is probably only required for tests,
# hence would be best move to a test fixture
REQUIRED_TABLES = ["TS009"] if os.getenv("ENV") == "dev" else None


# TODO - these regexes are probably only useful for table TS009.
# At present that is the only table we use using for any of the derived metrics
# In future we might need to structure these in a more scalable manner
SexCategory = Literal["female", "male", "total"]

sex_regexes: dict[SexCategory, re.Pattern[str]] = {
    "total": re.compile(r"Sex: All persons;"),
    "female": re.compile(r"Sex: Female;"),
    "male": re.compile(r"Sex: Male;"),
}

age_regex = re.compile(
    r"Age: (Aged (?P<age>\d\d?) years?;|(?P<baby>Aged under 1 year);|(?P<ninetyplus>Aged 90 years and over);) measures: Value"
)
all_ages_regex = re.compile(r"Age: Total; measures: Value")


def columns_selector(
    columns_list: Iterable[Any], age_range: list[int] | None, sex: SexCategory
):
    """
    Notes
    -----
    age_range includes the
    """
    sex_regex = sex_regexes[sex]

    columns_to_sum = []
    for col in columns_list:
        if not sex_regex.search(col):
            continue

        # No age range specified, so just look for the total column
        if age_range is None:
            match = all_ages_regex.search(col)
            if match:
                columns_to_sum.append(col)
            continue

        # Else look for the specific age range
        match = age_regex.search(col)
        if match and (
            (match.group("baby") and 0 in age_range)
            or (match.group("ninetyplus") and 90 in age_range)
            or (match.group("age") and int(match.group("age")) in age_range)
        ):
            columns_to_sum.append(col)

    return columns_to_sum


DERIVED_COLUMNS = [
    DerivedColumn(
        hxltag="#population+children+age5_17",
        column_select=lambda df: columns_selector(
            df.columns, list(range(5, 18)), "total"
        ),
        output_column_name="children_5_17",
        human_readable_name="Children aged 5 to 17",
    ),
    DerivedColumn(
        hxltag="#population+infants+age0_4",
        # TODO - THE CURRENT REGEXS DO NOT CAPTURE THOSE AGED < 1
        column_select=lambda df: columns_selector(
            df.columns,
            list(range(5)),
            "total",
        ),
        output_column_name="infants_0_4",
        human_readable_name="Infants aged 0 to 4",
    ),
    DerivedColumn(
        hxltag="#population+children+age0_17",
        column_select=lambda df: columns_selector(df.columns, list(range(18)), "total"),
        output_column_name="children_0_17",
        human_readable_name="Children aged 0 to 17",
    ),
    DerivedColumn(
        hxltag="#population+adults+f",
        output_column_name="adults_f",
        column_select=lambda df: columns_selector(
            df.columns, list(range(18, 91)), "female"
        ),
        human_readable_name="Female adults",
    ),
    DerivedColumn(
        hxltag="#population+adults+m",
        column_select=lambda df: columns_selector(
            df.columns, list(range(18, 91)), "male"
        ),
        output_column_name="adults_m",
        human_readable_name="Male adults",
    ),
    DerivedColumn(
        hxltag="#population+adults",
        column_select=lambda df: columns_selector(
            df.columns, list(range(18, 91)), "total"
        ),
        output_column_name="adults",
        human_readable_name="Adults",
    ),
    DerivedColumn(
        hxltag="#population+ind",
        column_select=lambda df: columns_selector(df.columns, None, "total"),
        output_column_name="individuals",
        human_readable_name="Total individuals",
    ),
]

# Lookup of `partition_key` (eg geom + source table id) to `DerivedColumn` (columns that can be derived from the source table)
DERIVED_COLUMN_SPECIFICATIONS: dict[str, list[DerivedColumn]] = {
    "ltla/TS009": DERIVED_COLUMNS,
}


class EnglandAndWales(Country):
    geo_levels: ClassVar[list[str]] = list(EW_CENSUS_GEO_LEVELS.keys())
    required_tables: list[str] | None = REQUIRED_TABLES
    country_metadata: ClassVar[CountryMetadata] = country

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
        At present this function will download each zipfile multiple times. This is a consequence
        of the fact that
        * Each partition is a unique combination of topic summary and geometry level.
        * Each zip file contains multiple files including every geometry level for a given topic
          summary.

        I cannot see an easy way to share a cached downloaded zip file where different files are
        then extracted for different partitions (given dagster can in-theory execute partitions
        in arbitrary order and even on a cluster of machines). However a better solution may exist.
        """
        partition_key = context.asset_partition_key_for_output()
        current_table = catalog[catalog["partition_key"] == partition_key]

        source_download_url = current_table["source_download_url"].to_numpy()[0]
        source_archive_file_path = current_table["source_archive_file_path"].to_numpy()[
            0
        ]

        # Keep the temp directory when developing, for debugging purposes
        # del_temp_dir = os.getenv("ENV") != "dev"
        with TemporaryDirectory() as temp_dir:  # pyright: ignore [reportCallIssue]
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
    ) -> MetricsOutput:
        _SEP = "_"
        partition_key = context.partition_key
        _geo_level = partition_key.split("/")[0]
        source_table = census_tables
        source_mmd = source_metric_metadata
        # source_column = source_mmd.parquet_column_name
        # context.log.debug(ic(source_table.columns))
        # context.log.debug(ic(source_column))
        # context.log.debug(ic(source_table.head()))
        context.log.debug(ic(len(source_table)))

        if len(source_table) == 0:
            # Source data not available
            msg = f"Source data not available for partition key: {partition_key}"
            context.log.warning(msg)
            return MetricsOutput(metadata=[], metrics=pd.DataFrame())

        # geo_id = EW_CENSUS_GEO_LEVELS[geo_level].census_table_column
        # The columns "date", "geography" and "geography code" appear to be common to all tables
        # TODO - Could we use the "date" column to update the mmds?
        geo_id = "geography code"
        source_table = source_table.rename(columns={geo_id: COL.GEO_ID.value}).drop(
            # Drop the "geography" column as it is the name, not the ID
            columns=["date", "geography"]
        )

        parquet_file_name = (
            f"{self.key_prefix}/metrics/"
            f"{''.join(c for c in partition_key if c.isalnum()) + '.parquet'}"
        )
        derived_mmd: list[MetricMetadata] = []

        # Create MMD for all of the existing columns
        for col in source_table.columns:
            # We do not need an MMD for the GEO_ID column
            if col == COL.GEO_ID.value:
                continue
            new_mmd = source_mmd.model_copy(deep=True)
            new_mmd.parent_metric_id = source_mmd.source_metric_id
            new_mmd.metric_parquet_path = parquet_file_name
            new_mmd.hxl_tag = "#population+details_unknown"
            new_mmd.parquet_column_name = str(col)
            new_mmd.human_readable_name = str(col)
            derived_mmd.append(new_mmd)

        # Now deal with the derived columns - create MMD and functions to calculate them
        new_column_funcs = {}

        # Filter function to sum the columns (which will be used in the loop below)
        def sum_cols_func(df, col_names: Iterable[str]):
            return df[col_names].sum(axis=1)
            #  sum([df[col] for col in col_names])

        try:
            metric_specs = DERIVED_COLUMN_SPECIFICATIONS[partition_key]
            for metric_spec in metric_specs:
                # Get the list of columns that need to be summed
                columns_to_sum = metric_spec.column_select(source_table)
                # ic(type(columns_to_sum))
                # ic(columns_to_sum)
                # Add details to the dict of new columns that will be created
                new_column_funcs[metric_spec.output_column_name] = partial(
                    sum_cols_func, col_names=columns_to_sum
                )

                # Create a new metric metadata object
                new_mmd = source_mmd.model_copy(deep=True)
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

        # Create a new table which only has the GEO_ID and the new columns
        new_table = source_table.assign(**new_column_funcs)

        ic(len(derived_mmd))
        ic(len(new_table.columns))

        # TODO - ADD METADATA to context
        context.add_output_metadata(
            metadata={
                "metadata_preview": MetadataValue.md(
                    metadata_to_dataframe(derived_mmd).head().to_markdown()
                ),
                "metrics_shape": f"{new_table.shape[0]} rows x {new_table.shape[1]} columns",
                "metrics_columns": MetadataValue.json(new_table.columns.to_list()),
                "metrics_preview": MetadataValue.md(new_table.head().to_markdown()),
            },
        )

        return MetricsOutput(metadata=derived_mmd, metrics=new_table)

    def _metrics(
        self,
        context,
        catalog: pd.DataFrame,
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
        geometry: list[GeometryOutput],
        data_publisher: DataPublisher,
    ) -> dict[str, SourceDataRelease]:
        source_data_releases = {}

        for geo_output in geometry:
            geo_metadata = geo_output.metadata
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

    def _geometry(self, context) -> list[GeometryOutput]:
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
                country_metadata=country,
            )
            geometries_raw: gpd.GeoDataFrame = gpd.read_file(
                level_details.data_download_url
            )

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
                GeometryOutput(
                    metadata=geometry_metadata,
                    gdf=geometries_gdf,
                    names_df=name_lookup_df,
                )
            )

        # Add output metadata
        # TODO, It is not clear that this is the best way to represent the metadata
        # Specifically, this assumes that the order of EW_CENSUS_GEO_LEVELS is based
        # on the hierarchy of the geometries, which may not be the case.
        example_geometry_output = geometries_to_return[0]
        first_metadata = example_geometry_output.metadata
        first_gdf = example_geometry_output.gdf
        first_names = example_geometry_output.names_df
        first_joined_gdf = first_gdf.merge(first_names, on="GEO_ID")
        ax = first_joined_gdf.plot(column="en", legend=False)
        ax.set_title(f"England & Wales 2021 {first_metadata.level}")
        md_plot = markdown_from_plot()
        context.add_output_metadata(
            metadata={
                "all_geom_levels": MetadataValue.md(
                    ",".join(
                        [
                            geom_output.metadata.level
                            for geom_output in geometries_to_return
                        ]
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
