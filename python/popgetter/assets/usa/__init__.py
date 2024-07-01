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
import geopandas as gpd
from popgetter.cloud_outputs import GeometryOutput, MetricsOutput
import pandas as pd
from typing import Any, ClassVar
from dagster import MetadataValue, asset
from functools import reduce
from .census_tasks import (
    get_geom_ids_table_for_summary,
    generate_variable_dictionary,
    generate_variable_dictionary,
    get_geom_ids_table_for_summary,
    get_summary_table_file_names,
    get_summary_table,
    extract_values_at_specified_levels,
    generate_variable_dictionary,
    select_estimates,
    select_errors,
)
from datetime import date
from more_itertools import batched
from icecream import ic

# from .config import ACS_METADATA, SUMMARY_LEVELS
from .census_tasks import ACS_METADATA, SUMMARY_LEVELS

SUMMARY_LEVEL_STRINGS = ["oneYear", "fiveYear"]
GEOMETRY_COL = "AFFGEOID"
METRICS_COL = "GEO_ID"

# For testing
REQUIRED_TABLES = [
    "acsdt1y2019-b01001.dat",
    "acsdt1y2019-b01001.dat",
    "acsdt1y2019-b01001a.dat",
    "acsdt1y2019-b01001b.dat",
    "acsdt1y2019-b01001d.dat",
]
BATCH_SIZE = 2

# For prod
REQUIRED_TABLES = None
BATCH_SIZE = 10


class USA(Country):
    country_metadata: ClassVar[CountryMetadata] = CountryMetadata(
        name_short_en="United States",
        name_official="United States of America",
        iso2="US",
        iso3="USA",
        iso3166_2=None,
    )
    required_tables: list[str] | None = None

    def _country_metadata(self, _context) -> CountryMetadata:
        return self.country_metadata

    def _data_publisher(
        self, _context, country_metadata: CountryMetadata
    ) -> DataPublisher:
        return DataPublisher(
            name="United States Census Bureau",
            url="https://www.census.gov/programs-surveys/acs",
            description=(
                """
                The United States Census Bureau, officially the Bureau of the Census, 
                is a principal agency of the U.S. Federal Statistical System, responsible 
                for producing data about the American people and economy.
                """
            ),
            countries_of_interest=[country_metadata.id],
        )

    def _catalog(self, context) -> pd.DataFrame:
        self.remove_all_partition_keys(context)
        catalog_list = []
        for year, _ in ACS_METADATA.items():
            # for geo_level, _ in metadata["geoms"].items():
            for summary_level in SUMMARY_LEVEL_STRINGS:
                for geo_level in SUMMARY_LEVELS:
                    # If year and summary level has no data, skip it.
                    if (ACS_METADATA[year][summary_level] is None) or (
                        summary_level == "oneYear"
                        and (geo_level == "tract" or geo_level == "block_group")
                    ):
                        continue

                    table_names_list = [
                        table_name
                        for table_name in get_summary_table_file_names(
                            year, summary_level
                        )
                        if (
                            REQUIRED_TABLES is not None
                            and table_name in REQUIRED_TABLES
                        )
                        or REQUIRED_TABLES is None
                    ]

                    table_names_list = list(batched(table_names_list, BATCH_SIZE))

                    # Catalog
                    table_names = pd.DataFrame({"table_names_batch": table_names_list})
                    # table_names["urls"] = table_names["table_names_batch"].apply(
                    #     lambda s: tuple(
                    #         [f"{summary_file_dir}/{table_name}" for table_name in s]
                    #     )
                    # )
                    table_names["year"] = year
                    table_names["summary_level"] = summary_level
                    table_names["geo_level"] = geo_level
                    table_names["batch"] = range(len(table_names_list))
                    table_names["partition_key"] = (
                        table_names["year"].astype(str)
                        + "/"
                        + table_names["summary_level"].astype(str)
                        + "/"
                        + table_names["geo_level"].astype(str)
                        + "/"
                        + table_names["batch"].astype(str)
                        # .apply(lambda x: x.split(".")[0])
                    )
                    catalog_list.append(table_names)

        catalog = pd.concat(catalog_list, axis=0).reset_index(drop=True)
        self.add_partition_keys(context, catalog["partition_key"].to_list())
        add_metadata(context, catalog, "Catalog")
        return catalog

    def _geometry(
        self, context
    ) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
        geometries_to_return = []
        for year, metadata in ACS_METADATA.items():
            context.log.debug(ic(year))
            context.log.debug(ic(metadata))
            names_col = ACS_METADATA[year]["geoIdCol"]
            # Combine fiveYear and oneYear geoIDs
            if year != 2020:
                geo_ids5 = get_geom_ids_table_for_summary(year, "fiveYear")
                geo_ids1 = get_geom_ids_table_for_summary(year, "oneYear")
                geo_ids = (
                    pd.concat(
                        [
                            geo_ids5,
                            geo_ids1[~geo_ids1[names_col].isin(geo_ids5[names_col])],
                        ],
                        axis=0,
                    )
                    .reset_index(drop=True)
                    .rename(columns={names_col: "GEO_ID", "NAME": "eng"})
                )
            else:
                geo_ids = (
                    get_geom_ids_table_for_summary(year, "fiveYear")
                    .rename(columns={names_col: "GEO_ID", "NAME": "eng"})
                    .loc[:, ["GEO_ID", "eng"]]
                )

            for geo_level, url in metadata["geoms"].items():
                geometry_metadata = GeometryMetadata(
                    country_metadata=self.country_metadata,
                    validity_period_start=date(year, 1, 1),
                    validity_period_end=date(year, 1, 1),
                    level=geo_level,
                    # TODO: what should hxl_tag be?
                    hxl_tag=geo_level,
                )

                region_geometries_raw: gpd.GeoDataFrame = gpd.read_file(url)

                # Copy names
                region_names = geo_ids.copy()
                region_geometries_raw = region_geometries_raw.dissolve(
                    by=GEOMETRY_COL
                ).reset_index()

                context.log.debug(ic(region_geometries_raw.head()))
                context.log.debug(ic(region_geometries_raw.columns))
                region_geometries = region_geometries_raw.rename(
                    columns={GEOMETRY_COL: "GEO_ID"}
                ).loc[:, ["geometry", "GEO_ID"]]
                context.log.debug(ic(region_geometries.head()))
                context.log.debug(ic(region_geometries.columns))

                # TODO: Merge names.
                # Check this step, is subetting to those with geo IDs correct?
                region_geometries = region_geometries.loc[
                    region_geometries["GEO_ID"].isin(region_names["GEO_ID"])
                ]

                # Rename cols for names
                region_names = region_names.loc[
                    # Subset to row in geoms file
                    region_names["GEO_ID"].isin(region_geometries["GEO_ID"]),
                    ["GEO_ID", "eng"],
                ].drop_duplicates()
                context.log.debug(ic(region_names.head()))
                context.log.debug(ic(region_names.columns))

                geometries_to_return.append(
                    GeometryOutput(
                        metadata=geometry_metadata,
                        gdf=region_geometries,
                        names_df=region_names,
                    )
                )

        return geometries_to_return

    def _source_data_releases(
        self, _context, geometry, data_publisher
    ) -> dict[str, SourceDataRelease]:
        source_data_releases = {}

        idx = 0
        for year, metadata in ACS_METADATA.items():
            for _, url in metadata["geoms"].items():
                for summary_level in ["oneYear", "fiveYear"]:
                    geo = geometry[idx]
                    source_data_release: SourceDataRelease = SourceDataRelease(
                        name=(
                            f"ACS {year} 1 year"
                            if summary_level == "oneYear"
                            else f"ACS {year} 5 year"
                        ),
                        date_published=date(year, 1, 1),
                        reference_period_start=date(year, 1, 1),
                        reference_period_end=date(year, 1, 1),
                        collection_period_start=date(year, 1, 1),
                        collection_period_end=date(year, 1, 1),
                        expect_next_update=date(year, 1, 1),
                        # TODO: should this be replaced with url from metadata?
                        # url="https://www.census.gov/programs-surveys/acs",
                        url=url,
                        data_publisher_id=data_publisher.id,
                        description="""
                            The American Community Survey (ACS) helps local officials,
                            community leaders, and businesses understand the changes
                            taking place in their communities. It is the premier source
                            for detailed population and housing information about our nation.
                        """,
                        geometry_metadata_id=geo.metadata.id,
                    )
                    source_data_releases[
                        f"{year}_{summary_level}_{geo.metadata.level}"
                    ] = source_data_release
                idx += 1

        return source_data_releases

    def _census_tables(self, context, catalog) -> pd.DataFrame:
        partition = context.asset_partition_key_for_output()
        ic(partition)
        ic(catalog.loc[catalog["partition_key"].eq(partition), "table_names_batch"])
        row = catalog.loc[catalog["partition_key"].eq(partition), :]
        table_names_batch = row.iloc[0]["table_names_batch"]
        year = row.iloc[0].loc["year"]
        summary_level = row.iloc[0].loc["summary_level"]
        geo_level = row.iloc[0].loc["geo_level"]

        # TODO: generate as an asset to cache result per year and summary_level
        geoids = get_geom_ids_table_for_summary(year, summary_level)
        census_tables = []
        for table_name in table_names_batch:
            df = get_summary_table(table_name, year, summary_level)
            values = extract_values_at_specified_levels(
                df, geoids, ACS_METADATA[int(year)]["geoIdCol"]
            )
            try:
                table = values[geo_level]
                context.log.info(ic(table))
                context.log.info(ic(table.columns))
                census_tables.append(table)
            except Exception as err:
                msg = (
                    f"Could not get table ({table_name}) at geo level ({geo_level}) "
                    f"for summary level ({summary_level}) in year ({year}) with "
                    f"error: {err}"
                )
                context.log.warning(msg)

        if len(census_tables) > 0:
            census_tables = reduce(
                lambda left, right: left.merge(
                    right, on=METRICS_COL, how="outer", validate="one_to_one"
                ),
                census_tables,
            )
        else:
            census_tables = pd.DataFrame()
            msg = (
                f"No tables at geo level ({geo_level}) "
                f"for summary level ({summary_level}) in year ({year})."
            )
            context.log.warning(msg)

        add_metadata(
            context, census_tables, title=context.asset_partition_key_for_output()
        )
        return census_tables

    def _source_metric_metadata(
        self,
        context,
        catalog: pd.DataFrame,
        source_data_releases: dict[str, SourceDataRelease],
    ) -> MetricMetadata: ...

    def make_partial_metric_metadata(
        self,
        column: str,
        variable_dictionary: pd.DataFrame,
        source_data_release: SourceDataRelease,
        partition_key: str,
        table_names: Any,
        year: str,
        summary_level: str,
    ) -> MetricMetadata:
        def column_to_variable(name: str) -> str:
            split = name.split("_")
            return split[0] + "_" + split[1][1:]

        def variable_to_column(variable: str, type: str = "M") -> str:
            split = variable.split("_")
            return split[0] + f"_{type}" + split[1]

        variable = column_to_variable(column)
        info = (
            variable_dictionary.loc[variable_dictionary["uniqueID"].eq(variable)]
            .iloc[0]
            .to_dict()
        )

        def gen_url(
            col: str, table_names: list[str], year: int, summary_level: str
        ) -> str:
            base = ACS_METADATA[year]["base"]
            summary_file_dir = base + ACS_METADATA[year][summary_level]["tables"]
            for table_name in table_names:
                table_id = table_name.split("-")[1].split(".")[0].upper()
                col_start = col.split("_")[0]
                if col_start == table_id:
                    return f"{summary_file_dir}/{table_name}"
            return "TBD"

        def gen_parquet_path(partition_key: str) -> str:
            return "/".join(
                [
                    self.key_prefix,
                    "metrics",
                    f"{''.join(c for c in partition_key if c.isalnum()) + '.parquet'}",
                ]
            )

        def gen_description(info: dict[str, str]) -> str:
            return "; ".join(
                [f"Key: {key}, Value: {value}" for key, value in info.items()]
            )

        def gen_hxl_tag(info: dict[str, str]) -> str:
            return (
                "#"
                + "".join(
                    [c for c in info["universe"].title() if c != " " and c.isalnum()]
                )
                + "+"
                + "+".join(
                    "".join(c for c in split.title() if c.isalnum() and c != " ")
                    for split in info["variableExtendedName"].split("|")
                )
            )

        def gen_human_readable_name() -> str:
            return (
                f"{info['universe']}, {year}, {summary_level}, {info['variableName']}"
            )

        return MetricMetadata(
            human_readable_name=gen_human_readable_name(),
            description=gen_description(info),
            hxl_tag=gen_hxl_tag(info),
            metric_parquet_path=gen_parquet_path(partition_key),
            parquet_column_name=column,
            parquet_margin_of_error_column=variable_to_column(variable, "E"),
            parquet_margin_of_error_file=variable_to_column(variable, "M"),
            potential_denominator_ids=None,
            # TODO: get value
            source_metric_id="TBD",
            parent_metric_id=None,
            source_data_release_id=source_data_release.id,
            # TODO: check this works
            source_download_url=gen_url(column, table_names, int(year), summary_level),
            source_archive_file_path=None,
            # TODO: get value
            source_documentation_url="TBD",
        )

    def create_derived_metrics(self):
        """
        Creates an asset providing the metrics derived from the census tables and the
        corresponding source metric metadata.
        """

        @asset(partitions_def=self.dataset_node_partition, key_prefix=self.key_prefix)
        def derived_metrics(
            context,
            catalog: pd.DataFrame,
            census_tables: pd.DataFrame,
            source_data_releases: dict[str, SourceDataRelease],
            # source_metric_metadata: MetricMetadata,
        ) -> MetricsOutput:

            partition_key = context.partition_key
            row = catalog[catalog["partition_key"] == partition_key].iloc[0].to_dict()
            year = row["year"]
            summary_level = row["summary_level"]
            geo_level = row["geo_level"]
            table_names = row["table_names_batch"]

            # TODO: consider refactoring as asset
            variable_dictionary = generate_variable_dictionary(year, summary_level)
            if census_tables.shape[0] == 0 or census_tables.shape[1] == 0:
                context.log.warning(f"No data found in parition: {partition_key}")
                return MetricsOutput(metadata=[], metrics=pd.DataFrame())

            metrics = census_tables.copy().set_index(METRICS_COL)

            if metrics.shape[1] == 0:
                context.log.warning(f"No metrics found in parition: {partition_key}")
                return MetricsOutput(metadata=[], metrics=pd.DataFrame())

            estimates = select_estimates(metrics)
            # TODO: No need to select errors, unless to check there is an error column
            # errors = select_errors(metrics)

            if estimates.shape[1] == 0:
                context.log.warning(f"No estimates found in parition: {partition_key}")
                return MetricsOutput(metadata=[], metrics=pd.DataFrame())

            variable_dictionary = generate_variable_dictionary(year, summary_level)
            derived_mmd = []
            for col in estimates.columns:
                metric_metadata = self.make_partial_metric_metadata(
                    col,
                    variable_dictionary,
                    source_data_releases[f"{year}_{summary_level}_{geo_level}"],
                    partition_key,
                    table_names,
                    year,
                    summary_level,
                )
                derived_mmd.append(metric_metadata)

            context.add_output_metadata(
                metadata={
                    "metadata_preview": MetadataValue.md(
                        metadata_to_dataframe(derived_mmd).head().to_markdown()
                    ),
                    "metrics_shape": f"{metrics.shape[0]} rows x {metrics.shape[1]} columns",
                    "metrics_preview": MetadataValue.md(metrics.head().to_markdown()),
                },
            )
            return MetricsOutput(metadata=derived_mmd, metrics=metrics)

        return derived_metrics

    # Implementation not required since overridden
    def _derived_metrics(self, census_tables): ...


# Assets
usa = USA()
country_metadata = usa.create_country_metadata()
data_publisher = usa.create_data_publisher()
geometry = usa.create_geometry()
source_data_releases = usa.create_source_data_releases()
catalog = usa.create_catalog()
census_tables = usa.create_census_tables()
derived_metrics = usa.create_derived_metrics()
metrics = usa.create_metrics()
