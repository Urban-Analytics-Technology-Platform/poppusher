from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import os

import pandas as pd
from dagster import AssetIn, MetadataValue, SpecificPartitionsPartitionMapping, asset
from icecream import ic
from rdflib import Graph, URIRef
from rdflib.namespace import DCAT, DCTERMS

from popgetter.assets.bel.utils import (
    DOWNLOAD_HANDLERS,
    filter_by_language,
    get_distribution_url,
    get_landpage_url,
    no_op_format_handler,
)
from popgetter.assets.country import Country
from popgetter.cloud_outputs import (
    GeometryOutput,
    MetricsOutput,
    send_to_geometry_sensor,
)
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
)
from popgetter.utils import add_metadata, markdown_from_plot
from functools import reduce
from popgetter.metadata import metadata_to_dataframe


KNOWN_FAILING_DATASETS = {
    # sqlite compressed as tar.gz
    "595",  # Census 2011 - Matrix of commutes by statistical sector
    # faulty zip file (confirmed by manual download)
    "2676",
    # Excel only (French and Dutch only)
    "2654",  # Geografische indelingen 2020
    "3961",  # Geografische indelingen 2021
    # AccessDB only!
    "4135",  # Enterprises subject to VAT according to legal form (English only)
    "4136",  # Enterprises subject to VAT according to employer class (English only)
}

@dataclass
class DerivedColumn:
    hxltag: str
    filter_func: Callable[[pd.DataFrame], pd.DataFrame]
    output_column_name: str
    human_readable_name: str

def nationality_to_string(n):
    if n == "ETR":
        return "non-Belgian"
    if n == "BEL":
        return "Belgian"
    raise ValueError

def married_status_to_string(cs):
    if cs == 1:
        return "single"
    if cs == 2:
        return "married"
    if cs == 3:
        return "widowed"
    if cs == 4:
        return "divorced"
    raise ValueError

DATASET_SPECIFICATION = {
    "4726": {
        # All geometries 2023
        "hxltag": "#geo+bounds+sector+2023",
        "geography_level": "statistical_sector",  # Lowest level
        "source_column": "",
        "geoid_column": "",
    },
    "4796": {
        # Population in statistical sectors, 2023
        "hxltag": "#population+adm5+total+2023",
        "geography_level": "statistical_sector",
        "source_column": "TOTAL",
        "geoid_column": "CD_SECTOR",
        # Specify how to generate derived metrics
        "pivot_columns": [],
        # For datasets that don't need to be pivot, these fields should just be
        # constants.
        "derived_hxl": "population+adm5+total+2023",
        "derived_human_readable_name": "Population, total, 2023",
    },
    "4689": {
        # Population by nationality, marital status, age, and sex in
        # municipalities, 2023
        "hxltag": "#population+adm4+total+2023",
        "geography_level": "municipality",
        "source_column": "MS_POPULATION",
        "geoid_column": "CD_REFNIS",
        # Specify how to generate derived metrics
        "pivot_columns": ["CD_NATLTY", "CD_CIV_STS", "CD_AGE", "CD_SEX"],
        # For datasets that require pivoting, these entries have to be
        # functions which take the values of the pivot columns in the correct
        # order
        "derived_hxl": lambda n, ms, a, s: f"#population+adm4+{s.lower()}+age{a}+{nationality_to_string(n).lower()}+{married_status_to_string(ms).lower()}",
        "derived_human_readable_name": lambda n, ms, a, s: f"Population, {nationality_to_string(n)}, {married_status_to_string(ms)}, {'female' if s == 'F' else 'male'}, and age {a}",
    },
}

REQUIRED_DATASETS = (None if os.getenv("ENV") == "PROD"
                     else list(DATASET_SPECIFICATION.keys()))

@dataclass
class BelgiumGeometryLevel:
    level: str
    hxl_tag: str
    geo_id_column: str
    name_columns: dict[str, str]  # keys = language codes, values = column names


BELGIUM_GEOMETRY_LEVELS = {
    "province": BelgiumGeometryLevel(
        level="province",
        hxl_tag="adm1",
        geo_id_column="cd_prov_refnis",
        name_columns={
            "nld": "tx_prov_descr_nl",
            "fra": "tx_prov_descr_fr",
            "deu": "tx_prov_descr_de",
        },
    ),
    "region": BelgiumGeometryLevel(
        level="region",
        hxl_tag="adm2",
        geo_id_column="cd_rgn_refnis",
        name_columns={
            "nld": "tx_rgn_descr_nl",
            "fra": "tx_rgn_descr_fr",
            "deu": "tx_rgn_descr_de",
        },
    ),
    "arrondisement": BelgiumGeometryLevel(
        level="arrondisement",
        hxl_tag="adm3",
        geo_id_column="cd_dstr_refnis",
        name_columns={
            "nld": "tx_adm_dstr_descr_nl",
            "fra": "tx_adm_dstr_descr_fr",
            "deu": "tx_adm_dstr_descr_de",
        },
    ),
    "municipality": BelgiumGeometryLevel(
        level="municipality",
        hxl_tag="adm4",
        geo_id_column="cd_munty_refnis",
        name_columns={
            "nld": "tx_munty_descr_nl",
            "fra": "tx_munty_descr_fr",
            "deu": "tx_munty_descr_de",
        },
    ),
    "statistical_sector": BelgiumGeometryLevel(
        level="statistical_sector",
        hxl_tag="adm5",
        geo_id_column="cd_sector",
        name_columns={
            "nld": "tx_sector_descr_nl",
            "fra": "tx_sector_descr_fr",
            "deu": "tx_sector_descr_de",
        },
    ),
}


class Belgium(Country):
    country_metadata: CountryMetadata = CountryMetadata(
        name_short_en="Belgium",
        name_official="Kingdom of Belgium",
        iso3="BEL",
        iso2="BE",
        iso3166_2=None,
    )

    def _country_metadata(self, _context) -> CountryMetadata:
        return self.country_metadata

    def _data_publisher(
        self, _context, country_metadata: CountryMetadata
    ) -> DataPublisher:
        return DataPublisher(
            name="Statbel",
            url="https://statbel.fgov.be/en",
            description="Statbel is the Belgian statistical office. It is part of the Federal Public Service Economy, SMEs, Self-employed and Energy.",
            countries_of_interest=[country_metadata.id],
        )

    @staticmethod
    def get_opendata_dataset_list() -> Graph:
        """
        Returns a list of all the tables available in the Statbel Open Data portal.

        This document is essential reading for understanding the structure of the data:
        https://github.com/belgif/inspire-dcat/blob/main/DCATAPprofil.en.md
        """
        CATALOG_URL = (
            "https://doc.statbel.be/publications/DCAT/DCAT_opendata_datasets.ttl"
        )
        graph = Graph()
        graph.parse(CATALOG_URL, format="ttl")
        return graph

    def _catalog(self, context) -> pd.DataFrame:
        self.remove_all_partition_keys(context)

        opendata_dataset_list = self.get_opendata_dataset_list()
        # Create the schema for the catalog
        catalog_summary = {
            "node": [],
            "human_readable_name": [],
            "description": [],
            "metric_parquet_path": [],
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

        # Loop over the datasets in the catalogue Graph
        CATALOG_ROOT = URIRef("http://data.gov.be/catalog/statbelopen")
        for dataset_id in opendata_dataset_list.objects(
            subject=CATALOG_ROOT, predicate=DCAT.dataset, unique=True
        ):
            catalog_summary["node"].append(str(dataset_id).removeprefix("https://statbel.fgov.be/node/"))
            catalog_summary["human_readable_name"].append(
                filter_by_language(
                    graph=opendata_dataset_list,
                    subject=dataset_id,
                    predicate=DCTERMS.title,
                )
            )
            catalog_summary["description"].append(
                filter_by_language(
                    opendata_dataset_list,
                    subject=dataset_id,
                    predicate=DCTERMS.description,
                )
            )

            # This is unknown at this stage
            catalog_summary["metric_parquet_path"].append(None)
            catalog_summary["parquet_margin_of_error_column"].append(None)
            catalog_summary["parquet_margin_of_error_file"].append(None)
            catalog_summary["potential_denominator_ids"].append(None)
            catalog_summary["parent_metric_id"].append(None)
            catalog_summary["source_data_release_id"].append(None)
            catalog_summary["parquet_column_name"].append(None)

            download_url, archive_file_path, format = get_distribution_url(
                opendata_dataset_list, dataset_id
            )
            catalog_summary["source_download_url"].append(download_url)
            catalog_summary["source_archive_file_path"].append(archive_file_path)
            catalog_summary["source_format"].append(format)

            catalog_summary["source_documentation_url"].append(
                get_landpage_url(opendata_dataset_list, dataset_id, language="en")
            )

        # Convert to dataframe and remove datasets which are known to fail
        catalog_df = pd.DataFrame(data=catalog_summary, dtype="string")
        catalog_df = catalog_df[~catalog_df["node"].isin(KNOWN_FAILING_DATASETS)]

        # If not production, restrict the catalog to only the required datasets
        if REQUIRED_DATASETS is not None:
            catalog_df = catalog_df[catalog_df["node"].isin(REQUIRED_DATASETS)]

        self.add_partition_keys(context, catalog_df["node"].to_list())
        add_metadata(context, catalog_df, "Catalog")
        return catalog_df

    def _census_tables(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        row = catalog[catalog["node"] == context.partition_key]
        format = row["source_format"].iloc[0]
        ic(format)
        handler = DOWNLOAD_HANDLERS.get(format, no_op_format_handler)
        return handler(context, row=row)

    def _geometry(self, _context) -> list[GeometryOutput]:
        err = "The geometry asset for Belgium has a custom implementation as it depends on the census_tables asset."
        raise NotImplementedError(err)

    def _source_data_releases(
        self, _context, geometry, data_publisher
    ) -> dict[str, SourceDataRelease]:
        return {
            geo_output.metadata.level: SourceDataRelease(
                name="StatBel Open Data",
                date_published=date(2015, 10, 22),
                reference_period_start=date(2015, 10, 22),
                reference_period_end=date(2015, 10, 22),
                collection_period_start=date(2015, 10, 22),
                collection_period_end=date(2015, 10, 22),
                expect_next_update=date(2022, 1, 1),
                url="https://statbel.fgov.be/en/open-data",
                description="TBC",
                data_publisher_id=data_publisher.id,
                geometry_metadata_id=geo_output.metadata.id,
            )
            for geo_output in geometry
        }

    def _source_metric_metadata(
        self,
        context,
        catalog: pd.DataFrame,
        source_data_releases: dict[str, SourceDataRelease],
    ) -> MetricMetadata:
        catalog_row = catalog[catalog["node"] == context.partition_key].to_dict(orient="records")[0]
        dataset_spec = DATASET_SPECIFICATION[context.partition_key]

        return MetricMetadata(
            human_readable_name=catalog_row["human_readable_name"],
            source_download_url=catalog_row["source_download_url"],
            source_archive_file_path=catalog_row["source_archive_file_path"],
            source_documentation_url=catalog_row["source_documentation_url"],
            source_data_release_id=source_data_releases[dataset_spec["geography_level"]].id,
            parent_metric_id=None,
            potential_denominator_ids=None,
            parquet_margin_of_error_file=None,
            parquet_margin_of_error_column=None,
            parquet_column_name=dataset_spec["source_column"],
            metric_parquet_path="__PLACEHOLDER__",
            hxl_tag=dataset_spec["hxltag"],
            description=catalog_row["description"].strip(),
            source_metric_id=dataset_spec["source_column"],
        )

    def _derived_metrics(
        self,
        context,
        census_tables: pd.DataFrame,
        source_metric_metadata: MetricMetadata,
    ) -> MetricsOutput:
        # Skip if we don't know what to do with this partition
        try:
            this_dataset_spec = DATASET_SPECIFICATION[context.partition_key]
        except KeyError:
            skip_reason = f"No action specified for partition key {context.partition_key}"
            context.log.warning(skip_reason)
            return MetricsOutput(metadata=[], metrics=pd.DataFrame())

        # Return empty metrics if the source table is empty, if the source
        # column is not present
        source_column = source_metric_metadata.parquet_column_name
        skip_reason = None
        if len(census_tables) == 0:
            skip_reason = "Skipping as input table is empty"
            context.log.warning(skip_reason)
            return MetricsOutput(metadata=[], metrics=pd.DataFrame())
        if source_column not in census_tables.columns:
            skip_reason = (f"Skipping as source column '{source_column}' is not"
                           f" present in the input table.")
            context.log.warning(skip_reason)
            return MetricsOutput(metadata=[], metrics=pd.DataFrame())

        # Assign parquet file name
        parquet_file_name = (
            f"{self.country_metadata.id}/metrics/"
            f"{''.join(c for c in context.partition_key if c.isalnum()) + '.parquet'}"
        )

        # Rename the geoID column to GEO_ID
        geo_id_col_name = this_dataset_spec["geoid_column"]
        census_tables = census_tables.rename(columns={geo_id_col_name: "GEO_ID"})

        # Generate derived metrics through pivoting
        if len(this_dataset_spec["pivot_columns"]) > 0:
            needed_columns = ["GEO_ID", source_column] + this_dataset_spec["pivot_columns"]
            df = census_tables[needed_columns]
            df = df.pivot(
                index="GEO_ID",
                columns=this_dataset_spec["pivot_columns"],
                values=source_column,
            )
            # Generate metadata structs
            derived_mmds = []
            for c in df.columns.to_flat_index():
                new_mmd = source_metric_metadata.copy()
                new_mmd.parent_metric_id = source_metric_metadata.source_metric_id
                new_mmd.metric_parquet_path = parquet_file_name
                new_mmd.hxl_tag = this_dataset_spec["derived_hxl"](*c)
                new_mmd.human_readable_name = this_dataset_spec["derived_human_readable_name"](*c)
                new_mmd.parquet_column_name = "_".join([str(x) for x in c])
                derived_mmds.append(new_mmd)
            # Rename columns 
            col_names = [m.parquet_column_name for m in derived_mmds]
            df.columns = col_names

        else:
            # No pivoting required. Just extract the column
            df = census_tables[["GEO_ID", source_column]]
            # Generate metadata struct
            new_mmd = source_metric_metadata.copy()
            new_mmd.parent_metric_id = source_metric_metadata.source_metric_id
            new_mmd.metric_parquet_path = parquet_file_name
            new_mmd.hxl_tag = this_dataset_spec["derived_hxl"]
            new_mmd.human_readable_name = this_dataset_spec["derived_human_readable_name"]
            new_mmd.parquet_column_name = source_column
            derived_mmds = [new_mmd]

        context.add_output_metadata(
            metadata={
                "metadata_preview": MetadataValue.md(
                    metadata_to_dataframe(derived_mmds).head().to_markdown()
                ),
                "metrics_shape": f"{df.shape[0]} rows x {df.shape[1]} columns",
                "metrics_preview": MetadataValue.md(df.head().to_markdown()),
            },
        )
        return MetricsOutput(metadata=derived_mmds, metrics=df)


# Create assets
bel = Belgium()
country_metadata = bel.create_country_metadata()
data_publisher = bel.create_data_publisher()
catalog = bel.create_catalog()
census_tables = bel.create_census_tables()


@send_to_geometry_sensor
@asset(
    ins={
        "sector_geometries": AssetIn(
            key=["bel", "census_tables"],
            partition_mapping=SpecificPartitionsPartitionMapping(
                ["4726"]
            ),
        ),
    },
    key_prefix="bel",
)
def geometry(context, sector_geometries) -> list[GeometryOutput]:
    """
    Produces the full set of data / metadata associated with Belgian
    municipalities. The outputs, in order, are:

    1. A DataFrame containing a serialised GeometryMetadata object.
    2. A GeoDataFrame containing the geometries of the municipalities.
    3. A DataFrame containing the names of the municipalities (in this case,
       they are in Dutch, French, and German).
    """
    geometries_to_return = []

    for level_details in BELGIUM_GEOMETRY_LEVELS.values():
        geometry_metadata = GeometryMetadata(
            country_metadata=bel.country_metadata,
            validity_period_start=date(2023, 1, 1),
            validity_period_end=date(2023, 12, 31),
            level=level_details.level,
            hxl_tag=level_details.hxl_tag,
        )

        region_geometries = (
            sector_geometries.dissolve(by=level_details.geo_id_column)
            .reset_index()
            .rename(columns={level_details.geo_id_column: "GEO_ID"})
            .loc[:, ["geometry", "GEO_ID"]]
        )
        ic(region_geometries.head())

        region_names = (
            sector_geometries.rename(
                columns={
                    level_details.geo_id_column: "GEO_ID",
                    level_details.name_columns["nld"]: "nld",
                    level_details.name_columns["fra"]: "fra",
                    level_details.name_columns["deu"]: "deu",
                }
            )
            .loc[:, ["GEO_ID", "nld", "fra", "deu"]]
            .drop_duplicates()
            .astype({"GEO_ID": str})
        )
        ic(region_names.head())

        geometries_to_return.append(
            GeometryOutput(
                metadata=geometry_metadata, gdf=region_geometries, names_df=region_names
            )
        )

    # Add output metadata
    first_output = geometries_to_return[0]
    first_joined_gdf = first_output.gdf.merge(first_output.names_df, on="GEO_ID")
    ax = first_joined_gdf.plot(column="nld", legend=False)
    ax.set_title(f"Belgium 2023 {first_output.metadata.level}")
    md_plot = markdown_from_plot()
    context.add_output_metadata(
        metadata={
            "all_geom_levels": MetadataValue.md(
                ",".join(
                    [geom_output.metadata.level for geom_output in geometries_to_return]
                )
            ),
            "first_geometry_plot": MetadataValue.md(md_plot),
            "first_names_preview": MetadataValue.md(
                first_output.names_df.head().to_markdown()
            ),
        }
    )

    return geometries_to_return


source_data_releases = bel.create_source_data_releases()
source_metric_metadata = bel.create_source_metric_metadata()
derived_metrics = bel.create_derived_metrics()
metrics = bel.create_metrics()
