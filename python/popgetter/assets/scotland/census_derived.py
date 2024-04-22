from __future__ import annotations

import pandas as pd
from dagster import (
    AssetIn,
    AssetOut,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
    multi_asset,
)

from ...metadata import MetricMetadata
from .scotland import add_metadata, dataset_node_partition


def get_lc1117sc_metric(
    lc1117sc: pd.DataFrame, col: str, subset: list[str]
) -> pd.DataFrame:
    lc1117sc_transformed = lc1117sc.rename(
        columns={"Unnamed: 0": "OA11CD", "Unnamed: 1": "Age Category"}
    )
    lc1117sc_transformed = lc1117sc_transformed.loc[
        ~lc1117sc_transformed["OA11CD"].str.startswith("S92"), :
    ]
    return (
        lc1117sc_transformed.loc[
            lc1117sc_transformed["Age Category"].isin(subset),
            ["OA11CD", col],
        ]
        .groupby("OA11CD")
        .agg("sum")
        .rename(columns={col: "Count"})
    )


ALL_PEOPLE = ["All people"]
INFANTS_AGE_0_TO_4 = ["0 to 4"]
CHILDREN_AGE_0_TO_17 = ["0 to 4", "5 to 9", "10 to 11", "12 to 14", "15", "16 to 17"]
CHILDREN_AGE_5_TO_17 = ["5 to 9", "10 to 11", "12 to 14", "15", "16 to 17"]
ADULTS = [
    "18 to 19",
    "20 to 24",
    "25 to 29",
    "30 to 34",
    "35 to 39",
    "40 to 44",
    "45 to 49",
    "50 to 54",
    "55 to 59",
    "60 to 64",
    "65 to 69",
    "70 to 74",
    "75 to 79",
    "80 to 84",
    "85 to 89",
    "90 to 94",
    "95 and over",
]

needed_dataset_list = [
    {
        # Population by OA11, Period: 2011
        "partition_key": "2011/OA11/LC1117SC",
        "hxltag": "#population+oa11+2011",
        # TODO: this partition key does not have a single column for source
        "source_column": "",
    }
]
needed_dataset_partions_keys: list[str] = [
    r["partition_key"] for r in needed_dataset_list
]
needed_dataset_mapping = SpecificPartitionsPartitionMapping(
    needed_dataset_partions_keys
)
needed_dataset_partition = StaticPartitionsDefinition(needed_dataset_partions_keys)

# Using HXL tags for variable names (https://hxlstandard.org/standard/1-1final/dictionary/#tag_population)
_derived_columns: list[dict] = [
    {
        "partition_key": "2011/OA11/LC1117SC",
        "hxltag": "population_children_age5_17",
        "filter_func": lambda df: get_lc1117sc_metric(
            df, "All people", CHILDREN_AGE_5_TO_17
        ),
    },
    {
        "partition_key": "2011/OA11/LC1117SC",
        "hxltag": "population_infants_age0_4",
        "filter_func": lambda df: get_lc1117sc_metric(
            df, "All people", INFANTS_AGE_0_TO_4
        ),
    },
    {
        "partition_key": "2011/OA11/LC1117SC",
        "hxltag": "population_children_age0_17",
        "filter_func": lambda df: get_lc1117sc_metric(
            df, "All people", CHILDREN_AGE_0_TO_17
        ),
    },
    {
        "partition_key": "2011/OA11/LC1117SC",
        "hxltag": "population_adults_f",
        "filter_func": lambda df: get_lc1117sc_metric(df, "Females", ADULTS),
    },
    {
        "partition_key": "2011/OA11/LC1117SC",
        "hxltag": "population_adults_m",
        "filter_func": lambda df: get_lc1117sc_metric(df, "Males", ADULTS),
    },
    {
        "partition_key": "2011/OA11/LC1117SC",
        "hxltag": "population_adults",
        "filter_func": lambda df: get_lc1117sc_metric(df, "All people", ADULTS),
    },
    {
        "partition_key": "2011/OA11/LC1117SC",
        "hxltag": "population_ind",
        "filter_func": lambda df: get_lc1117sc_metric(df, "All people", ALL_PEOPLE),
    },
]

derived_columns = pd.DataFrame(
    _derived_columns, columns=["node", "hxltag", "filter_func"]
)


# record = {
#     "resolution": resolution,
#     "catalog_resolution": table_metadata["catalog_resolution"],
#     "source": source,
#     "url": url,
#     "file_name": Path(source) / file_name,
#     "table_name": table_name,
#     "year": table_metadata["year"],
#     # Use constructed name of description and coverage
#     "human_readable_name": table_metadata["human_readable_name"],
#     "source_metric_id": None,
#     # Use catalog_metadata description
#     "description": table_metadata["description"],
#     "hxl_tag": None,
#     "metric_parquet_file_url": None,
#     "parquet_column_name": None,
#     "parquet_margin_of_error_column": None,
#     "parquet_margin_of_error_file": None,
#     "potential_denominator_ids": None,
#     "parent_metric_id": None,
#     # TODO: check this is not an ID but a name
#     "source_data_release_id": table_metadata["census_release"],
#     "source_download_url": url,
#     # TODO: what should this be?
#     "source_archive_file_path": None,
#     "source_documentation_url": URL_CATALOG_METADATA,
# }


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


@asset(
    ins={
        "catalog": AssetIn(partition_mapping=needed_dataset_mapping),
    },
)
def filter_needed_catalog(
    context, needed_datasets, catalog: pd.DataFrame
) -> pd.DataFrame:
    needed_df = needed_datasets.merge(catalog, how="inner", on="partition_key")
    add_metadata(context, needed_df, "needed_df")
    return needed_df


@asset
def needed_datasets(context) -> pd.DataFrame:
    needed_df = pd.DataFrame(
        needed_dataset_list,
        columns=["partition_key", "hxltag", "source_column", "derived_columns"],
        dtype="string",
    )
    add_metadata(context, needed_df, "needed_datasets")
    return needed_df


@multi_asset(
    ins={
        "individual_census_table": AssetIn(partition_mapping=needed_dataset_mapping),
        "filter_needed_catalog": AssetIn(),
    },
    outs={
        "source_table": AssetOut(),
        "source_mmd": AssetOut(),
    },
    partitions_def=dataset_node_partition,
)
def get_enriched_tables_scotland(
    context, individual_census_table, filter_needed_catalog
) -> tuple[pd.DataFrame, MetricMetadata]:
    partition_keys = context.asset_partition_keys_for_input(
        input_name="individual_census_table"
    )
    output_partition = context.asset_partition_key_for_output("source_table")
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
        filter_needed_catalog["partition_key"].eq(output_partition)
    ]
    catalog_row = catalog_row.to_dict(orient="index")
    catalog_row = catalog_row.popitem()[1]
    result_mmd = census_table_metadata(catalog_row)
    return result_df, result_mmd


# TODO: from here


@multi_asset(
    partitions_def=dataset_node_partition,
    ins={
        "source_table": AssetIn(partition_mapping=needed_dataset_mapping),
        "source_mmd": AssetIn(partition_mapping=needed_dataset_mapping),
    },
    outs={"derived_table": AssetOut(), "derived_mmds": AssetOut()},
)
def transform_data(
    context,
    source_table: dict[str, pd.DataFrame],
    source_mmd: dict[str, MetricMetadata],
) -> tuple[pd.DataFrame, list[MetricMetadata]]:
    partition_key = context.asset_partition_key_for_output("derived_table")
    census_table = source_table[partition_key]
    parent_mmd = source_mmd[partition_key]
    # source_column = parent_mmd.parquet_column_name
    metrics = derived_columns[derived_columns["partition_key"].eq(partition_key)]
    new_series: list[pd.Series] = []
    new_mmds: list[MetricMetadata] = []
    for row_tuple in metrics.itertuples():
        _, _, col_name, group_by_column, filter = row_tuple
        new_series.append(filter(census_table))
        new_mmd = parent_mmd.copy()
        new_mmd.parent_metric_id = parent_mmd.source_metric_id
        new_mmd.hxl_tag = col_name
        new_mmds.append(new_mmd)
    new_table: pd.DataFrame = pd.concat(new_series, axis=1)
    add_metadata(context, new_table, "derived_table")
    return new_table, new_mmds
