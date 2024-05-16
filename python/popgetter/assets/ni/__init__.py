from __future__ import annotations

import io
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from functools import reduce

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dagster import (
    AssetIn,
    DynamicPartitionsDefinition,
    IdentityPartitionMapping,
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
)
from icecream import ic

from popgetter.assets.common import Country
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
    metadata_to_dataframe,
)
from popgetter.utils import add_metadata, markdown_from_plot

PARTITION_NAME = "uk-ni_dataset_nodes"
REQUIRED_TABLES = [
    "MS-A09",
]
REQUIRED_TABLES_REGEX = "|".join(REQUIRED_TABLES)

# TODO
REQUIRED_RELEASES = [""]
# GENERAL_METHODS_URL = "https://www.scotlandscensus.gov.uk/media/jx2lz54n/scotland-s_census_2011_general_report.pdf"

# TODO: get these are correct dates
CENSUS_REFERENCE_DATE = date(2021, 3, 21)
CENSUS_COLLECTION_DATE = date(2021, 3, 21)
CENSUS_EXPECT_NEXT_UPDATE = date(2031, 1, 1)
CENSUS_REFERENCE_DATE = date(2021, 3, 21)
# https://www.nisra.gov.uk/publications/census-2021-outputs-prospectus:
# 9.30 am on 21 February 2023 for DZ and SDZ and District Electoral Areas
CENSUS_PUBLICATION_DATE = date(2023, 2, 21)


@dataclass
class NIGeometryLevel:
    level: str
    hxl_tag: str
    geo_id_column: str
    name_columns: dict[str, str]  # keys = language codes, values = column names
    url: str


NI_GEO_LEVELS = {
    "DZ21": NIGeometryLevel(
        level="DZ21",
        hxl_tag="TBD",
        geo_id_column="DZ2021_cd",
        name_columns={"en": "DZ2021_nm"},
        url="https://www.nisra.gov.uk/sites/nisra.gov.uk/files/publications/geography-dz2021-esri-shapefile.zip",
    )
}

# Full list of geographies, see metadata:
# https://build.nisra.gov.uk/en/metadata/dataset?d=PEOPLE
GEO_LEVELS = [
    "LGD14",  # Local Government District 2014
    # "URBAN_STATUS", # Urban Status
    # "HEALTH_TRUST", # Health and Social Care Trust
    # "PARLCON24", # Parliamentary Constituency 2024
    # "DEA14", # District Electoral Area 2014
    "SDZ21",  # Census 2021 Super Data Zone
    "DZ21",  # Census 2021 Data Zone
]


def get_nodes_and_links() -> dict[str, dict[str, str]]:
    SCHEME_AND_HOST = "https://build.nisra.gov.uk"
    urls = [
        "".join([SCHEME_AND_HOST, url.get("href")])
        for url in BeautifulSoup(
            requests.get(SCHEME_AND_HOST + "/en/standard").content, features="lxml"
        ).find_all("a")
        if str(url.get("href")).startswith("/en/standard")
    ]
    nodes: dict[str, dict[str, str]] = {}
    for url in urls:
        soup = BeautifulSoup(requests.get(url).content, features="lxml")
        nodes[url] = {
            "table_url": list(
                set(
                    [
                        "".join([SCHEME_AND_HOST, link.get("href")])
                        for link in soup.find_all("a")
                        if "table.csv?" in link.get("href")
                    ]
                )
            )[0],
            "metadata_url": list(
                set(
                    [
                        "".join([SCHEME_AND_HOST, link.get("href")])
                        for link in soup.find_all("a")
                        if "table.csv-metadata" in link.get("href")
                    ]
                )
            )[0],
        }
    return nodes


class NorthernIreland(Country):
    partition_name: str = PARTITION_NAME
    geo_levels: list[str] = GEO_LEVELS
    required_tables: list[str] = REQUIRED_TABLES

    # def catalog(self, context: AssetExecutionContext) -> pd.DataFrame:
    def catalog(self, context) -> pd.DataFrame:
        """
        A catalog for NI can be generated in two ways:
        1. With flexible table builder:
                https://build.nisra.gov.uk/en/
            with metadata chosen from:
                https://build.nisra.gov.uk/en/metadata
        2. Or through enumerating the ready-made tables:
            https://build.nisra.gov.uk/en/standard
            However, some level of
        """
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
            "table_schema": [],
        }
        nodes = get_nodes_and_links()

        def add_resolution(s: str, geo_level: str) -> str:
            s_split = s.split("?")
            query_params = s_split[1].split("&")
            if query_params[0].startswith("d="):
                query_params = "&".join(
                    [query_params[0], f"v={geo_level}", *query_params[2:]]
                )
            else:
                query_params = "&".join([f"v={geo_level}", *query_params[1:]])
            out_url = "?".join([s_split[0], query_params])
            ic(out_url)
            return out_url

        for node_url, node_items in nodes.items():
            for geo_level in self.geo_levels:
                metadata = requests.get(node_items["metadata_url"]).json()
                table_id = metadata["dc:title"].split(":")[0]
                # Skip if not required
                if table_id not in self.required_tables:
                    continue

                catalog_summary["node"].append(node_url)
                catalog_summary["table_id"].append(table_id)
                catalog_summary["geo_level"].append(geo_level)
                catalog_summary["partition_key"].append(f"{geo_level}/{table_id}")
                catalog_summary["human_readable_name"].append(metadata["dc:title"])
                catalog_summary["description"].append(metadata["dc:description"])
                catalog_summary["metric_parquet_file_url"].append(None)
                catalog_summary["parquet_column_name"].append(None)
                catalog_summary["parquet_margin_of_error_column"].append(None)
                catalog_summary["parquet_margin_of_error_file"].append(None)
                catalog_summary["potential_denominator_ids"].append(None)
                catalog_summary["parent_metric_id"].append(None)
                catalog_summary["source_data_release_id"].append(None)
                catalog_summary["source_download_url"].append(
                    add_resolution(metadata["url"], geo_level)
                )
                catalog_summary["source_format"].append(None)
                catalog_summary["source_archive_file_path"].append(None)
                catalog_summary["source_documentation_url"].append(node_url)
                catalog_summary["table_schema"].append(metadata["tableSchema"])

        catalog_df = pd.DataFrame.from_records(catalog_summary)
        context.instance.add_dynamic_partitions(
            partitions_def_name=self.partition_name,
            partition_keys=catalog_df["partition_key"].to_list(),
        )

        add_metadata(context, catalog_df, "Catalog")
        return catalog_df

    def census_tables(
        self, context, catalog: pd.DataFrame, partition: str
    ) -> pd.DataFrame:
        ic(partition)
        ic(catalog.loc[catalog["partition_key"].eq(partition), "source_download_url"])
        url = catalog.loc[
            catalog["partition_key"].eq(partition), "source_download_url"
        ].iloc[0]
        return pd.read_csv(io.BytesIO(requests.get(url).content), encoding="utf8")

    def source_table(self) -> pd.DataFrame:
        return pd.DataFrame()


country: CountryMetadata = CountryMetadata(
    name_short_en="Northern Ireland",
    name_official="Northern Ireland",
    iso3="GBR",
    iso2="GB",
    iso3166_2="GB-NIR",
)

publisher: DataPublisher = DataPublisher(
    name="NISRA",
    url="https://www.nisra.gov.uk/",
    description="The Northern Ireland Statistics and Research Agency (NISRA), which incorporates the General Register Office (GRO), is an executive agency within the Department of Finance (NI) and was established on 1 April 1996.",
    countries_of_interest=[country.id],
)


key_prefix = "uk-ni"

ni = NorthernIreland()

dataset_node_partition = DynamicPartitionsDefinition(name=PARTITION_NAME)


@asset
def catalog(context) -> pd.DataFrame:
    return ni.catalog(context)


@asset(partitions_def=dataset_node_partition)
# def census_tables(context: AssetExecutionContext, catalog) -> pd.DataFrame:
def census_tables(context, catalog) -> pd.DataFrame:
    census_table = ni.census_tables(
        context, catalog, context.asset_partition_key_for_output()
    )
    add_metadata(context, census_table, title=context.asset_partition_key_for_output())
    return census_table


@asset
# @asset(io_manager_key="geometry_io_manager", key_prefix=key_prefix)
def geometry(context) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
    # TODO: This is almost identical to Belgium so can probably be refactored to common
    # function with config of releases and languages
    level_details = NI_GEO_LEVELS["DZ21"]

    geometries_to_return = []
    for level_details in NI_GEO_LEVELS.values():
        # TODO: get correct values
        geometry_metadata = GeometryMetadata(
            validity_period_start=CENSUS_COLLECTION_DATE,
            validity_period_end=CENSUS_COLLECTION_DATE,
            level=level_details.level,
            hxl_tag=level_details.hxl_tag,
        )
        region_geometries_raw = (
            gpd.read_file(level_details.url)
            .dissolve(by=level_details.geo_id_column)
            .reset_index()
        )
        context.log.debug(ic(region_geometries_raw.head()))
        region_geometries = region_geometries_raw.rename(
            columns={level_details.geo_id_column: "GEO_ID"}
        ).loc[:, ["geometry", "GEO_ID"]]
        region_names = (
            region_geometries_raw.rename(
                columns={
                    level_details.geo_id_column: "GEO_ID",
                    level_details.name_columns["en"]: "en",
                }
            )
            .loc[:, ["GEO_ID", "en"]]
            .drop_duplicates()
        )
        geometries_to_return.append(
            (geometry_metadata, region_geometries, region_names)
        )

    # Add output metadata
    first_metadata, first_gdf, first_names = geometries_to_return[0]
    first_joined_gdf = first_gdf.merge(first_names, on="GEO_ID")
    ax = first_joined_gdf.plot(column="en", legend=False)
    ax.set_title(f"NI 2023 {first_metadata.level}")
    md_plot = markdown_from_plot(plt)
    context.add_output_metadata(
        metadata={
            "all_geom_levels": MetadataValue.md(
                ",".join([metadata.level for metadata, _, _ in geometries_to_return])
            ),
            "first_geometry_plot": MetadataValue.md(md_plot),
            "first_names_preview": MetadataValue.md(first_names.head().to_markdown()),
        }
    )

    return geometries_to_return


@asset()
def source_data_release(
    context, geometry: list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]
) -> SourceDataRelease:
    source_data_releases = []
    for geo_metadata, _, _ in geometry:
        # TODO: update with dates from config
        source_data_release: SourceDataRelease = SourceDataRelease(
            name="Census 2021",
            date_published=date(2014, 2, 27),
            reference_period_start=CENSUS_REFERENCE_DATE,
            reference_period_end=CENSUS_REFERENCE_DATE,
            collection_period_start=CENSUS_COLLECTION_DATE,
            collection_period_end=CENSUS_COLLECTION_DATE,
            expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
            url="https://www.nisra.gov.uk/publications/census-2021-outputs-prospectus",
            data_publisher_id=publisher.id,
            description="TBC",
            geometry_metadata_id=geo_metadata.id,
        )
        source_data_releases.append(source_data_release)
    # TODO: update for multiple source data releases
    return source_data_releases[0]


# @asset(partitions_def=dataset_node_partition)
# def source_mmd(
#     context,
#     catalog: pd.DataFrame,
#     source_data_release: SourceDataRelease,
# ) -> list[MetricMetadata]:
#     source_metadata_from_catalog(catalog)


# TODO: check if this is a simpler approach?
# @asset(partitions_def=dataset_node_partition)
# def source_tables(
#     context: AssetExecutionContext, census_tables: pd.DataFrame
# ) -> pd.DataFrame:
#     if context.partition_key not in DERIVED_COLUMN_SPECIFICATIONS.keys():
#         raise ValueError(f"Specified partition '{context.partition_key}' not handled")
#     return census_tables


@dataclass
class DerivedColumn:
    hxltag: str
    filter_func: Callable[[pd.DataFrame], pd.DataFrame]
    output_column_name: str
    human_readable_name: str


# The keys of this dict are the nodes (i.e. partition keys). The values are a
# list of all columns of data derived from this node.
age_code = "Age Code"
sex_code = "Sex Code"
DERIVED_COLUMN_SPECIFICATIONS: dict[str, (str, list[DerivedColumn])] = {  # type: ignore
    "DZ21/MS-A09": (
        "Census 2021 Data Zone Code",
        [
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
                    f"{age_code} >= 18 and {sex_code} == 'F'"
                ),
                output_column_name="adults_f",
                human_readable_name="Female adults",
            ),
            DerivedColumn(
                hxltag="#population+adults+m",
                filter_func=lambda df: df.query(
                    f"{age_code} >= 18 and {sex_code} == 'M'"
                ),
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
        ],
    )
}
_needed_dataset_nodes = list(set([key for key in DERIVED_COLUMN_SPECIFICATIONS.keys()]))
needed_dataset_mapping = SpecificPartitionsPartitionMapping(_needed_dataset_nodes)
needed_dataset_partition = StaticPartitionsDefinition(_needed_dataset_nodes)


def census_table_metadata(
    catalog_row: dict[str, str], source_data_release: SourceDataRelease
) -> MetricMetadata:
    return MetricMetadata(
        human_readable_name=catalog_row["human_readable_name"],
        source_download_url=catalog_row["source_download_url"],
        source_archive_file_path=catalog_row["source_archive_file_path"],
        source_documentation_url=catalog_row["source_documentation_url"],
        source_data_release_id=source_data_release.id,
        # TODO - this is a placeholder
        parent_metric_id="unknown_at_this_stage",
        potential_denominator_ids=None,
        parquet_margin_of_error_file=None,
        parquet_margin_of_error_column=None,
        parquet_column_name=catalog_row["source_column"],
        # TODO - this is a placeholder
        metric_parquet_path="unknown_at_this_stage",
        hxl_tag=catalog_row["hxltag"],
        description=catalog_row["description"],
        source_metric_id=catalog_row["hxltag"],
    )


@asset(
    ins={
        "census_tables": AssetIn(partition_mapping=needed_dataset_mapping),
        "catalog": AssetIn(),
        "source_data_release": AssetIn(),
    },
    partitions_def=dataset_node_partition,
)
def source_metrics_by_partition(
    context,
    census_tables: dict[str, pd.DataFrame],
    catalog: pd.DataFrame,
    # TODO: generalise to list or dict of SourceDataReleases as there may be
    # tables in here that are not at the same release level
    source_data_release: SourceDataRelease,
    # TODO: return an intermediate type instead of MetricMetadata
) -> tuple[MetricMetadata, pd.DataFrame]:
    input_partition_keys = context.asset_partition_keys_for_input(
        input_name="census_tables"
    )
    output_partition_key = context.partition_key

    if output_partition_key not in input_partition_keys:
        skip_reason = f"Skipping as requested partition {output_partition_key} is not part of the 'needed' partitions {input_partition_keys}"
        context.log.warning(skip_reason)
        raise RuntimeError(skip_reason)

    try:
        result_df = census_tables[output_partition_key]
    except KeyError:
        err_msg = (
            f"Partition key {output_partition_key} not found in census_tables\n"
            f"Available keys are {census_tables.keys()}"
        )
        raise ValueError(err_msg) from None

    catalog_row = catalog[catalog["node"] == output_partition_key].to_dict(
        orient="records"
    )[0]

    # catalog_row = catalog[catalog["partition_key"].eq(parition_key)].iloc[0, :]
    result_mmd = census_table_metadata(catalog_row, source_data_release)

    return result_mmd, result_df


@asset(
    partitions_def=dataset_node_partition,
    ins={
        "source_metrics_by_partition": AssetIn(
            partition_mapping=IdentityPartitionMapping()
        ),
    },
)
def derived_metrics_by_partition(
    context,
    source_metrics_by_partition: tuple[MetricMetadata, pd.DataFrame],
) -> tuple[list[MetricMetadata], pd.DataFrame]:
    node = context.partition_key

    source_mmd, source_table = source_metrics_by_partition
    source_column = source_mmd.parquet_column_name
    assert source_column in source_table.columns
    assert len(source_table) > 0

    try:
        geo_id_col_name, metric_specs = DERIVED_COLUMN_SPECIFICATIONS[node]
    except KeyError:
        skip_reason = (
            f"Skipping as no derived columns are to be created for node {node}"
        )
        context.log.warning(skip_reason)
        raise RuntimeError(skip_reason)

    # Rename the geoID column to GEO_ID
    source_table = source_table.rename(columns={geo_id_col_name: "GEO_ID"})

    derived_metrics: list[pd.DataFrame] = []
    derived_mmd: list[MetricMetadata] = []

    parquet_file_name = "".join(c for c in node if c.isalnum()) + ".parquet"

    for metric_spec in metric_specs:
        new_table = (
            source_table.pipe(metric_spec.filter_func)
            .groupby(by="GEO_ID", as_index=True)
            .sum()
            .rename(columns={source_column: metric_spec.output_column_name})
            .filter(items=["GEO_ID", metric_spec.output_column_name])
        )
        derived_metrics.append(new_table)

        new_mmd = source_mmd.copy()
        new_mmd.parent_metric_id = source_mmd.source_metric_id
        new_mmd.metric_parquet_path = parquet_file_name
        new_mmd.hxl_tag = metric_spec.hxltag
        new_mmd.parquet_column_name = metric_spec.output_column_name
        new_mmd.human_readable_name = metric_spec.human_readable_name
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
            "metrics_preview": MetadataValue.md(joined_metrics.head().to_markdown()),
        },
    )

    return derived_mmd, joined_metrics
