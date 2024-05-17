from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import reduce

import pandas as pd
from dagster import (
    AssetIn,
    IdentityPartitionMapping,
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
)
from icecream import ic

from popgetter.metadata import MetricMetadata, SourceDataRelease, metadata_to_dataframe

from .belgium import asset_prefix
from .census_tables import dataset_node_partition

_needed_dataset = [
    {
        # Population by Statistical sector, Period: 2023
        "node": "https://statbel.fgov.be/node/4796",
        "hxltag": "#population+total+2023",
        "source_column": "TOTAL",
    },
    {
        # Statistical sectors 2023
        "node": "https://statbel.fgov.be/node/4726",
        "hxltag": "#geo+bounds+sector+2023",
        "source_column": "",
    },
    {
        # Population by Statistical sector, Period: 2016
        "node": "https://statbel.fgov.be/node/1437",
        "hxltag": "#population+total+2016",
        "source_column": "POPULATION",
    },
    {
        # Population by Municipalities, Period: 2023
        "node": "https://statbel.fgov.be/node/4689",
        "hxltag": "#population+admn1+total+2023",
        "source_column": "MS_POPULATION",
    },
]

_needed_dataset_nodes: list[str] = [r["node"] for r in _needed_dataset]
needed_dataset_mapping = SpecificPartitionsPartitionMapping(_needed_dataset_nodes)
needed_dataset_partition = StaticPartitionsDefinition(_needed_dataset_nodes)


@dataclass
class DerivedColumn:
    hxltag: str
    filter_func: Callable[[pd.DataFrame], pd.DataFrame]
    output_column_name: str
    human_readable_name: str


# The keys of this dict are the nodes (i.e. partition keys). The values are a
# list of all columns of data derived from this node.
DERIVED_COLUMN_SPECIFICATIONS: dict[str, tuple[str, list[DerivedColumn]]] = {
    "https://statbel.fgov.be/node/4689": (
        "CD_REFNIS",
        [
            DerivedColumn(
                hxltag="#population+children+age5_17",
                filter_func=lambda df: df.query("CD_AGE >= 5 and CD_AGE < 18"),
                output_column_name="children_5_17",
                human_readable_name="Children aged 5 to 17",
            ),
            DerivedColumn(
                hxltag="#population+infants+age0_4",
                filter_func=lambda df: df.query("CD_AGE >= 0 and CD_AGE < 5"),
                output_column_name="infants_0_4",
                human_readable_name="Infants aged 0 to 4",
            ),
            DerivedColumn(
                hxltag="#population+children+age0_17",
                filter_func=lambda df: df.query("CD_AGE >= 0 and CD_AGE < 18"),
                output_column_name="children_0_17",
                human_readable_name="Children aged 0 to 17",
            ),
            DerivedColumn(
                hxltag="#population+adults+f",
                filter_func=lambda df: df.query("CD_AGE >= 18 and CD_SEX == 'F'"),
                output_column_name="adults_f",
                human_readable_name="Female adults",
            ),
            DerivedColumn(
                hxltag="#population+adults+m",
                filter_func=lambda df: df.query("CD_AGE >= 18 and CD_SEX == 'M'"),
                output_column_name="adults_m",
                human_readable_name="Male adults",
            ),
            DerivedColumn(
                hxltag="#population+adults",
                filter_func=lambda df: df.query("CD_AGE >= 18"),
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


@asset(key_prefix=asset_prefix)
def needed_datasets(context) -> pd.DataFrame:
    needed_df = pd.DataFrame(
        _needed_dataset,
        columns=["node", "hxltag", "source_column", "derived_columns"],
        dtype="string",
    )

    # Now add some metadata to the context
    context.add_output_metadata(
        # Metadata can be any key-value pair
        metadata={
            "num_records": len(needed_df),
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in needed_df.columns.to_list()])
            ),
            "columns_types": MetadataValue.md(needed_df.dtypes.to_markdown()),
            "preview": MetadataValue.md(needed_df.to_markdown()),
        }
    )

    return needed_df


def make_census_table_metadata(
    catalog_row: dict, source_data_release: SourceDataRelease
) -> MetricMetadata:
    return MetricMetadata(
        human_readable_name=catalog_row["human_readable_name"],
        source_download_url=catalog_row["source_download_url"],
        source_archive_file_path=catalog_row["source_archive_file_path"],
        source_documentation_url=catalog_row["source_documentation_url"],
        source_data_release_id=source_data_release.id,
        parent_metric_id=None,
        potential_denominator_ids=None,
        parquet_margin_of_error_file=None,
        parquet_margin_of_error_column=None,
        parquet_column_name=catalog_row["source_column"],
        metric_parquet_path="__PLACEHOLDER__",
        hxl_tag=catalog_row["hxltag"],
        description=catalog_row["description"],
        source_metric_id=catalog_row["source_column"],
    )


@asset(
    key_prefix=asset_prefix,
    ins={
        "catalog_as_dataframe": AssetIn(partition_mapping=needed_dataset_mapping),
    },
)
def filter_needed_catalog(
    context, needed_datasets, catalog_as_dataframe: pd.DataFrame
) -> pd.DataFrame:
    ic(needed_datasets.head())
    ic(needed_datasets.columns)
    ic(needed_datasets.dtypes)

    needed_df = needed_datasets.merge(catalog_as_dataframe, how="inner", on="node")

    context.add_output_metadata(
        metadata={
            "num_records": len(needed_df),
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in needed_df.columns.to_list()])
            ),
            "preview": MetadataValue.md(needed_df.to_markdown()),
        }
    )

    return needed_df


@asset(
    ins={
        "individual_census_table": AssetIn(
            key_prefix=asset_prefix, partition_mapping=needed_dataset_mapping
        ),
        "filter_needed_catalog": AssetIn(key_prefix=asset_prefix),
        "source_data_releases": AssetIn(key_prefix=asset_prefix),
    },
    partitions_def=dataset_node_partition,
    key_prefix=asset_prefix,
)
def source_metrics_by_partition(
    context,
    individual_census_table: dict[str, pd.DataFrame],
    filter_needed_catalog: pd.DataFrame,
    # TODO: generalise to list or dict of SourceDataReleases as there may be
    # tables in here that are not at the same release level
    # E.g. keys as Geography level ID
    source_data_releases: dict[str, SourceDataRelease],
    # TODO: return an intermediate type instead of MetricMetadata
) -> tuple[MetricMetadata, pd.DataFrame]:
    input_partition_keys = context.asset_partition_keys_for_input(
        input_name="individual_census_table"
    )
    output_partition_key = context.partition_key

    if output_partition_key not in input_partition_keys:
        skip_reason = f"Skipping as requested partition {output_partition_key} is not part of the 'needed' partitions {input_partition_keys}"
        context.log.warning(skip_reason)
        raise RuntimeError(skip_reason)

    try:
        result_df = individual_census_table[output_partition_key]
    except KeyError:
        err_msg = (
            f"Partition key {output_partition_key} not found in individual_census_table\n"
            f"Available keys are {individual_census_table.keys()}"
        )
        raise ValueError(err_msg) from None

    catalog_row = filter_needed_catalog[
        filter_needed_catalog["node"] == output_partition_key
    ].to_dict(orient="records")[0]

    # TODO: refine upon more general level handling with derived column config.
    # This config is currently called `DERIVED_COLUMN_SPECIFICATIONS` here and the
    # level can also be included there.
    key = "municipality"
    result_mmd = make_census_table_metadata(catalog_row, source_data_releases[key])

    return result_mmd, result_df


@asset(
    partitions_def=dataset_node_partition,
    ins={
        "source_metrics_by_partition": AssetIn(
            key_prefix=asset_prefix, partition_mapping=IdentityPartitionMapping()
        ),
    },
    key_prefix=asset_prefix,
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
        raise RuntimeError(skip_reason) from None

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


@asset(
    ins={
        "derived_metrics_by_partition": AssetIn(
            key_prefix=asset_prefix,
            partition_mapping=SpecificPartitionsPartitionMapping(
                ["https://statbel.fgov.be/node/4689"]
            ),
        ),
    },
    key_prefix=asset_prefix,
)
def metrics(
    context, derived_metrics_by_partition: tuple[list[MetricMetadata], pd.DataFrame]
) -> list[tuple[str, list[MetricMetadata], pd.DataFrame]]:
    """
    This asset exists solely to aggregate all the derived tables into one
    single unpartitioned asset, which the downstream publishing tasks can use.

    Right now it is a bit boring because it only relies on one partition, but
    it could be extended when we have more data products.
    """
    mmds, table = derived_metrics_by_partition
    filepath = mmds[0].metric_parquet_path

    context.add_output_metadata(
        metadata={
            "num_metrics": len(mmds),
            "num_parquets": 1,
        },
    )

    return [(filepath, mmds, table)]
