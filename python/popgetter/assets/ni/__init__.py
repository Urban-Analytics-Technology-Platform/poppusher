from __future__ import annotations

import io
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from functools import reduce
from typing import ClassVar

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dagster import (
    MetadataValue,
)
from icecream import ic

import popgetter
from popgetter.assets.country import Country
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
    metadata_to_dataframe,
)
from popgetter.utils import add_metadata, markdown_from_plot


@dataclass
class NIGeometryLevel:
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


# Geometry levels to include
# Full list of geographies, see metadata:
# https://build.nisra.gov.uk/en/metadata/dataset?d=PEOPLE
# - "LGD14",  # Local Government District 2014
# - "URBAN_STATUS", # Urban Status
# - "HEALTH_TRUST", # Health and Social Care Trust
# - "PARLCON24", # Parliamentary Constituency 2024
# - "DEA14", # District Electoral Area 2014
# - "SDZ21",  # Census 2021 Super Data Zone
# - "DZ21",  # Census 2021 Data Zone
NI_GEO_LEVELS = {
    "DZ21": NIGeometryLevel(
        level="DZ21",
        hxl_tag="TBD",
        geo_id_column="DZ2021_cd",
        census_table_column="Census 2021 Data Zone Code",
        name_columns={"en": "DZ2021_nm"},
        url="https://www.nisra.gov.uk/sites/nisra.gov.uk/files/publications/geography-dz2021-esri-shapefile.zip",
        lookup_url=None,
        lookup_sheet=None,
        left_on=None,
        right_on=None,
    ),
    "SDZ21": NIGeometryLevel(
        level="SDZ21",
        hxl_tag="TBD",
        geo_id_column="SDZ2021_cd",
        census_table_column="Census 2021 Super Data Zone Code",
        name_columns={"en": "SDZ2021_nm"},
        url="https://www.nisra.gov.uk/sites/nisra.gov.uk/files/publications/geography-sdz2021-esri-shapefile.zip",
        lookup_url=None,
        lookup_sheet=None,
        left_on=None,
        right_on=None,
    ),
    "LGD14": NIGeometryLevel(
        level="LGD14",
        hxl_tag="TBD",
        geo_id_column="LGD2014_cd",
        census_table_column="Local Government District 2014 Code",
        name_columns={"en": "LGD2014_name"},
        url="https://www.nisra.gov.uk/sites/nisra.gov.uk/files/publications/geography-dz2021-esri-shapefile.zip",
        lookup_url="https://www.nisra.gov.uk/sites/nisra.gov.uk/files/publications/geography-data-zone-and-super-data-zone-lookups.xlsx",
        lookup_sheet="DZ2021_lookup",
        left_on="DZ2021_cd",
        right_on="DZ2021_code",
    ),
}

# Required tables
REQUIRED_TABLES = ["MS-A09"] if os.getenv("ENV") == "dev" else None

# 2021 census collection date
CENSUS_COLLECTION_DATE = date(2021, 3, 21)


def get_nodes_and_links() -> dict[str, dict[str, str]]:
    """Extracts the URLs for census tables and metadata for ready-made tables."""
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
            "table_url": next(
                iter(
                    [
                        "".join([SCHEME_AND_HOST, link.get("href")])
                        for link in soup.find_all("a")
                        if "table.csv?" in link.get("href")
                    ]
                )
            ),
            "metadata_url": next(
                iter(
                    [
                        "".join([SCHEME_AND_HOST, link.get("href")])
                        for link in soup.find_all("a")
                        if "table.csv-metadata" in link.get("href")
                    ]
                )
            ),
        }
    return nodes


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

DERIVED_COLUMN_SPECIFICATIONS: dict[str, list[DerivedColumn]] = {
    "DZ21/MS-A09": DERIVED_COLUMNS,
    "SDZ21/MS-A09": DERIVED_COLUMNS,
}


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


class NorthernIreland(Country):
    key_prefix: str
    geo_levels: ClassVar[list[str]] = list(NI_GEO_LEVELS.keys())
    required_tables: list[str] | None = REQUIRED_TABLES

    def _country_metadata(self, _context) -> CountryMetadata:
        return CountryMetadata(
            name_short_en="Northern Ireland",
            name_official="Northern Ireland",
            iso3="GBR",
            iso2="GB",
            iso3166_2="GB-NIR",
        )

    def _data_publisher(
        self, _context, country_metadata: CountryMetadata
    ) -> DataPublisher:
        return DataPublisher(
            name="NISRA",
            url="https://www.nisra.gov.uk/",
            description=(
                "The Northern Ireland Statistics and Research Agency (NISRA), which "
                "incorporates the General Register Office (GRO), is an executive agency "
                "within the Department of Finance (NI) and was established on 1 April 1996."
            ),
            countries_of_interest=[country_metadata.id],
        )

    def _catalog(self, context) -> pd.DataFrame:
        """
        A catalog for NI can be generated in two ways:
        1. With flexible table builder:
                https://build.nisra.gov.uk/en/
            with metadata chosen from:
                https://build.nisra.gov.uk/en/metadata
        2. Or through enumerating the ready-made tables:
            https://build.nisra.gov.uk/en/standard
            However, for some geographical resolutions, ready-made tables may
            not be available due to data confidentiality.
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
        self.remove_all_partition_keys(context)

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
                if (
                    self.required_tables is not None
                    and table_id not in self.required_tables
                ):
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
        self.add_partition_keys(context, catalog_df["partition_key"].to_list())

        add_metadata(context, catalog_df, "Catalog")
        return catalog_df

    def _census_tables(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        partition = context.asset_partition_key_for_output()
        ic(partition)
        ic(catalog.loc[catalog["partition_key"].eq(partition), "source_download_url"])
        url = catalog.loc[
            catalog["partition_key"].eq(partition), "source_download_url"
        ].iloc[0]
        census_table = pd.read_csv(
            io.BytesIO(requests.get(url).content), encoding="utf8"
        )
        add_metadata(
            context, census_table, title=context.asset_partition_key_for_output()
        )
        return census_table

    def _geometry(
        self, context
    ) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
        # TODO: This is almost identical to Belgium so can probably be refactored to common
        # function with config of releases and languages
        geometries_to_return = []
        for level_details in NI_GEO_LEVELS.values():
            # TODO: get correct values
            geometry_metadata = GeometryMetadata(
                validity_period_start=CENSUS_COLLECTION_DATE,
                validity_period_end=CENSUS_COLLECTION_DATE,
                level=level_details.level,
                hxl_tag=level_details.hxl_tag,
            )
            region_geometries_raw: gpd.GeoDataFrame = gpd.read_file(level_details.url)
            if level_details.lookup_url is not None:
                lookup = pd.read_excel(
                    level_details.lookup_url, sheet_name=level_details.lookup_sheet
                )
                region_geometries_raw = region_geometries_raw.merge(
                    lookup,
                    left_on=level_details.left_on,
                    right_on=level_details.right_on,
                    how="outer",
                )

            region_geometries_raw = region_geometries_raw.dissolve(
                by=level_details.geo_id_column
            ).reset_index()

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
        ax.set_title(f"NI 2021 {first_metadata.level}")
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

    def _source_data_releases(
        self, _context, geometry, data_publisher
    ) -> dict[str, SourceDataRelease]:
        source_data_releases = {}
        for geo_metadata, _, _ in geometry:
            source_data_release: SourceDataRelease = SourceDataRelease(
                name="Census 2021",
                # https://www.nisra.gov.uk/publications/census-2021-outputs-prospectus:
                # 9.30 am on 21 February 2023 for DZ and SDZ and District Electoral Areas
                date_published=date(2023, 2, 21),
                reference_period_start=date(2021, 3, 21),
                reference_period_end=date(2021, 3, 21),
                collection_period_start=CENSUS_COLLECTION_DATE,
                collection_period_end=CENSUS_COLLECTION_DATE,
                expect_next_update=date(2031, 1, 1),
                url="https://www.nisra.gov.uk/publications/census-2021-outputs-prospectus",
                data_publisher_id=data_publisher.id,
                description="TBC",
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
            and partition_key not in DERIVED_COLUMN_SPECIFICATIONS.keys()
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

        geo_level = partition_key.split("/")[0]
        source_table = SourceTable(
            # TODO: how programmatically do this
            hxltag="TBD",
            geo_level=geo_level,
            geo_column=NI_GEO_LEVELS[geo_level].geo_id_column,
            source_column="Count",
        )

        return census_table_metadata(
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
        SEP = "_"
        partition_key = context.partition_key
        geo_level = partition_key.split("/")[0]
        source_table = census_tables
        source_mmd = source_metric_metadata
        source_column = source_mmd.parquet_column_name
        assert source_column in source_table.columns
        assert len(source_table) > 0

        geo_id = NI_GEO_LEVELS[geo_level].census_table_column
        source_table = source_table.rename(columns={geo_id: "GEO_ID"}).drop(
            columns=geo_id.replace("Code", "Label")
        )

        parquet_file_name = (
            "".join(c for c in partition_key if c.isalnum()) + ".parquet"
        )
        derived_metrics: list[pd.DataFrame] = []
        derived_mmd: list[MetricMetadata] = []

        try:
            metric_specs = DERIVED_COLUMN_SPECIFICATIONS[partition_key]
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
        except KeyError:
            skip_reason = f"Skipping as no derived columns are to be created for node {partition_key}"
            context.log.warning(skip_reason)

        # Get all other metrics from table as is pivoted
        def pivot_df(df: pd.DataFrame, end: str) -> tuple[list[str], pd.DataFrame]:
            # Variables are either code or label, only keep the case for given 'end'
            cols = (
                [col for col in df.columns if col.endswith(end)]
                + ["GEO_ID"]
                + ["Count"]
            )
            pivot_cols = [col for col in cols if col not in ["GEO_ID", "Count"]]
            ic(cols)
            ic(pivot_cols)
            ic(df.columns)
            ic(df.head())
            pivot = df[cols].pivot_table(
                index="GEO_ID", columns=pivot_cols, values="Count", aggfunc="sum"
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
            out_cols = [col.replace(var_type, "").strip() for col in pivot_cols]
            return out_cols, pivot

        # Pivot for codes and labels
        for var_type in ["Code", "Label"]:
            out_cols, new_table = pivot_df(source_table, var_type)
            ic(new_table)
            for metric_col in new_table.columns:
                metric_df = new_table.loc[:, metric_col].to_frame()
                ic(metric_df)
                derived_metrics.append(metric_df)
                new_mmd = source_mmd.copy()
                new_mmd.parent_metric_id = source_mmd.source_metric_id
                new_mmd.metric_parquet_path = parquet_file_name
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

    def _metrics(
        self, context, catalog: pd.DataFrame
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
                    [ni.key_prefix, "derived_metrics"], partition_key=partition_key
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


# Assets
ni = NorthernIreland("uk-ni")
country_metadata = ni.create_country_metadata()
data_publisher = ni.create_data_publisher()
geometry = ni.create_geometry()
source_data_releases = ni.create_source_data_releases()
catalog = ni.create_catalog()
census_tables = ni.create_census_tables()
source_metric_metadata = ni.create_source_metric_metadata()
derived_metrics = ni.create_derived_metrics()
metrics = ni.create_metrics()
