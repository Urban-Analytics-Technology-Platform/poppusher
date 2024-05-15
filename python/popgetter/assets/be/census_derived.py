from __future__ import annotations

import pandas as pd
from dagster import (
    AssetIn,
    AssetOut,
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
    multi_asset,
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

# Using HXL tags for variable names (https://hxlstandard.org/standard/1-1final/dictionary/#tag_population)
_derived_columns: list[dict] = [
    {
        "node": "https://statbel.fgov.be/node/4689",
        "hxltag": "population_children_age5_17",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df: (df["CD_AGE"] >= 5) & (df["CD_AGE"] < 18),
    },
    {
        "node": "https://statbel.fgov.be/node/4689",
        "hxltag": "population_infants_age0_4",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df: (df["CD_AGE"] <= 4),
    },
    {
        "node": "https://statbel.fgov.be/node/4689",
        "hxltag": "population_children_age0_17",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df: (df["CD_AGE"] >= 0) & (df["CD_AGE"] < 18),
    },
    {
        "node": "https://statbel.fgov.be/node/4689",
        "hxltag": "population_adults_f",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df: (df["CD_AGE"] > 18) & (df["CD_SEX"] == "F"),
    },
    {
        "node": "https://statbel.fgov.be/node/4689",
        "hxltag": "population_adults_m",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df: (df["CD_AGE"] > 18) & (df["CD_SEX"] == "M"),
    },
    {
        "node": "https://statbel.fgov.be/node/4689",
        "hxltag": "population_adults",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df: (df["CD_AGE"] > 18),
    },
    {
        "node": "https://statbel.fgov.be/node/4689",
        "hxltag": "population_ind",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df: (df["CD_AGE"] >= 0),
    },
]

derived_columns = pd.DataFrame(
    _derived_columns, columns=["node", "hxltag", "group_by_column", "filter_func"]
)


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
        # TODO - this is a placeholder
        parent_metric_id="unknown_at_this_stage",
        potential_denominator_ids=None,
        parquet_margin_of_error_file=None,
        parquet_margin_of_error_column=None,
        parquet_column_name=catalog_row["source_column"],
        # TODO - this is a placeholder
        metric_parquet_file_url="unknown_at_this_stage",
        hxl_tag=catalog_row["hxltag"],
        description=catalog_row["description"],
        source_metric_id=catalog_row["hxltag"],
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

    # Now add some metadata to the context
    context.add_output_metadata(
        # Metadata can be any key-value pair
        metadata={
            "num_records": len(needed_df),
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in needed_df.columns.to_list()])
            ),
            "preview": MetadataValue.md(needed_df.to_markdown()),
        }
    )

    return needed_df


def census_table_metadata(catalog_row: dict) -> MetricMetadata:
    return MetricMetadata(
        human_readable_name=catalog_row["human_readable_name"],
        source_download_url=catalog_row["source_download_url"],
        source_archive_file_path=catalog_row["source_archive_file_path"],
        source_documentation_url=catalog_row["source_documentation_url"],
        source_data_release_id="TODO",
        # TODO - this is a placeholder
        parent_metric_id="unknown_at_this_stage",
        potential_denominator_ids=None,
        parquet_margin_of_error_file=None,
        parquet_margin_of_error_column=None,
        parquet_column_name=catalog_row["source_column"],
        # TODO - this is a placeholder
        metric_parquet_file_url="unknown_at_this_stage",
        hxl_tag=catalog_row["hxltag"],
        description=catalog_row["description"],
        source_metric_id=catalog_row["hxltag"],
    )


@multi_asset(
    ins={
        "individual_census_table": AssetIn(
            key_prefix=asset_prefix, partition_mapping=needed_dataset_mapping
        ),
        "filter_needed_catalog": AssetIn(key_prefix=asset_prefix),
    },
    outs={
        "source_table": AssetOut(key_prefix=asset_prefix),
        "source_mmd": AssetOut(key_prefix=asset_prefix),
    },
    partitions_def=dataset_node_partition,
)
def get_enriched_tables(
    context, individual_census_table, filter_needed_catalog
) -> tuple[pd.DataFrame, MetricMetadata]:
    ic(context)
    partition_keys = context.asset_partition_keys_for_input(
        input_name="individual_census_table"
    )
    output_partition = context.asset_partition_key_for_output("source_table")
    ic(partition_keys)
    ic(len(partition_keys))
    ic(output_partition)
    ic(type(output_partition))

    if output_partition not in partition_keys:
        err_msg = f"Requested partition {output_partition} not found in the subset of 'needed' partitions {partition_keys}"
        raise ValueError(err_msg)

    if output_partition not in individual_census_table:
        err_msg = (
            f"Partition key {output_partition} not found in individual_census_table\n"
            f"Available keys are {individual_census_table.keys()}"
        )
        raise ValueError(err_msg)

    result_df = individual_census_table[output_partition]
    catalog_row = filter_needed_catalog[
        filter_needed_catalog["node"].eq(output_partition)
    ]
    ic(catalog_row)
    ic(type(catalog_row))
    catalog_row = catalog_row.to_dict(orient="index")
    ic(catalog_row)
    ic(type(catalog_row))
    catalog_row = catalog_row.popitem()[1]
    ic(catalog_row)
    ic(type(catalog_row))

    result_mmd = census_table_metadata(catalog_row)

    return result_df, result_mmd


@multi_asset(
    partitions_def=dataset_node_partition,
    ins={
        "source_data_release": AssetIn(key_prefix=asset_prefix),
        "source_table": AssetIn(
            key_prefix=asset_prefix, partition_mapping=needed_dataset_mapping
        ),
        "source_mmd": AssetIn(
            key_prefix=asset_prefix, partition_mapping=needed_dataset_mapping
        ),
    },
    outs={
        "derived_table": AssetOut(key_prefix=asset_prefix),
        "derived_mmds": AssetOut(key_prefix=asset_prefix),
    },
)
def pivot_data(
    context,
    source_data_release: SourceDataRelease,
    source_table: dict[str, pd.DataFrame],
    source_mmd: dict[str, MetricMetadata],
) -> tuple[pd.DataFrame, list[MetricMetadata]]:
    node = context.asset_partition_key_for_output("derived_table")

    census_table = source_table[node]
    parent_mmd = source_mmd[node]

    ic(census_table.columns)
    ic(parent_mmd.parquet_column_name)
    assert parent_mmd.parquet_column_name in census_table.columns
    assert len(census_table) > 0

    ic(census_table.head())
    ic(parent_mmd)

    source_column = parent_mmd.parquet_column_name
    metrics = derived_columns[derived_columns["node"].eq(node)]

    # TODO, check whether it is necessary to forcibly remove columns that are not
    # meaningful for the aggregation.

    new_table: pd.DataFrame = pd.DataFrame()

    new_mmds: list[MetricMetadata] = []

    for row_tuple in metrics.itertuples():
        ic(row_tuple)
        _, _, col_name, group_by_column, filter = row_tuple
        new_col_def = {col_name: pd.NamedAgg(column=source_column, aggfunc="sum")}
        subset = census_table.loc[filter]
        ic(subset.head())
        ic(len(subset))
        temp_table: pd.DataFrame = subset.groupby(
            by=group_by_column, as_index=True
        ).agg(
            func=None,
            **new_col_def,  # type: ignore TODO, don't know why pyright is complaining here
        )

        new_mmd = parent_mmd.copy()
        new_mmd.source_data_release_id = source_data_release.id
        new_mmd.parent_metric_id = parent_mmd.source_metric_id
        new_mmd.hxl_tag = col_name
        new_mmds.append(new_mmd)

        if len(new_table) == 0:
            new_table = temp_table
        else:
            new_table = new_table.merge(
                temp_table, left_index=True, right_index=True, how="inner"
            )

    context.add_output_metadata(
        output_name="derived_table",
        metadata={
            "num_records": len(new_table),
            "metrics_preview": MetadataValue.md(new_table.head().to_markdown()),
        },
    )
    context.add_output_metadata(
        output_name="derived_mmds",
        metadata={
            "num_records": len(new_mmds),
            "metadata_preview": MetadataValue.md(
                metadata_to_dataframe(new_mmds).head().to_markdown()
            ),
        },
    )

    return new_table, new_mmds
