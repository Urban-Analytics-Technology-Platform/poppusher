
from .census_tables import catalog_as_dataframe, source, dataset_node_partition
import pandas as pd
from .belgium import asset_prefix, country

from icecream import ic
from dagster import AssetIn, AssetOut, asset, MetadataValue, SpecificPartitionsPartitionMapping, StaticPartitionsDefinition, multi_asset
from popgetter.metadata import MetricMetadata



_needed_dataset = [
        {
            # Population by Statistical sector, Period: 2023
            "node" : "https://statbel.fgov.be/node/4796",
            "hxltag": "#population+total+2023",
            "source_column": "TOTAL",
        },
        {
            # Statistical sectors 2023
            "node" : "https://statbel.fgov.be/node/4726",
            "hxltag": "#geo+bounds+sector+2023",
            "source_column": "",
        },
        {
            # Population by Statistical sector, Period: 2016
            "node": "https://statbel.fgov.be/node/1437",
            "hxltag": "#population+total+2016",
            "source_column": "MS_POPULATION",

        },
    ]

# Using HXL tags for variable names (https://hxlstandard.org/standard/1-1final/dictionary/#tag_population)
_derived_columns : list[dict] = [
    {
        "node": "https://statbel.fgov.be/node/1437",
        "hxltag": "population_children_age5_17",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df : (df["CD_AGE"] >= 5) & (df["CD_AGE"] < 18),
    },
    {
        "node": "https://statbel.fgov.be/node/1437",
        "hxltag": "population_infants_age0_4",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df : (df["CD_AGE"] <= 4),
    },
    {
        "node": "https://statbel.fgov.be/node/1437",
        "hxltag": "population_children_age0_17",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df : (df["CD_AGE"] >= 0) & (df["CD_AGE"] < 18),
    },
    {
        "node": "https://statbel.fgov.be/node/1437",
        "hxltag": "population_adults_f",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df : (df["CD_AGE"] > 18) & (df["CD_SEX"] == "F"),
    },
    {
        "node": "https://statbel.fgov.be/node/1437",
        "hxltag": "population_adults_m",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df : (df["CD_AGE"] > 18) & (df["CD_SEX"] == "M"),
    },
    {
        "node": "https://statbel.fgov.be/node/1437",
        "hxltag": "population_adults",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df : (df["CD_AGE"] > 18),
    },
    {
        "node": "https://statbel.fgov.be/node/1437",
        "hxltag": "population_ind",
        "group_by_column": "CD_REFNIS",
        "filter_func": lambda df : (df["CD_AGE"] >= 0),
    },
]

derived_columns = pd.DataFrame(_derived_columns, columns=["node", "hxltag", "group_by_column", "filter_func"])

_needed_dataset_nodes : list[str] = [r["node"] for r in _needed_dataset]

needed_dataset_mapping = SpecificPartitionsPartitionMapping(_needed_dataset_nodes)

needed_dataset_partition = StaticPartitionsDefinition(_needed_dataset_nodes)


@asset(key_prefix=asset_prefix)
def needed_datasets(context) -> pd.DataFrame:

    # datasets_details = [
    #     {
    #         # Population by Statistical sector, Period: 2023
    #         "node" : "https://statbel.fgov.be/node/4796",
    #         "hxltag": "#population+total+2023",
    #         "source_column": "TOTAL",
    #     },
    #     {
    #         # Statistical sectors 2023
    #         "node" : "https://statbel.fgov.be/node/4726",
    #         "hxltag": "#geo+bounds+sector+2023",
    #         "source_column": "",
    #     },
    #     {
    #         # Population by Statistical sector, Period: 2016
    #         "node": "https://statbel.fgov.be/node/1437",
    #         "hxltag": "#population+total+2016",
    #         "source_column": "MS_POPULATION",
    #     },
    # ]

    needed_df = pd.DataFrame(_needed_dataset, columns=["node", "hxltag", "source_column", "derived_columns"], dtype="string")

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


def census_table_metadata(context, catalog_row) -> MetricMetadata:
    ic(catalog_row["human_readable_name"])
    ic(catalog_row["source_download_url"])
    ic(catalog_row["source_archive_file_path"])
    ic(catalog_row["source_documentation_url"])
    ic(catalog_row["parent_metric_id"])
    ic(catalog_row["source_column"])
    ic(catalog_row["hxltag"])
    ic(catalog_row["description"])


    mmd = MetricMetadata(
        human_readable_name = catalog_row["human_readable_name"],
        source_download_url = catalog_row["source_download_url"],
        source_archive_file_path = catalog_row["source_archive_file_path"],
        source_documentation_url = catalog_row["source_documentation_url"],
        source_data_release_id = source.id,
        # TODO - this is a placeholder
        parent_metric_id = "unknown_at_this_stage",
        potential_denominator_ids = None,
        parquet_margin_of_error_file = None,
        parquet_margin_of_error_column = None,
        parquet_column_name = catalog_row["source_column"],
        # TODO - this is a placeholder
        metric_parquet_file_url = "unknown_at_this_stage",
        hxl_tag = catalog_row["hxltag"],
        description = catalog_row["description"],
        source_metric_id=catalog_row["hxltag"],
    )

    return mmd

@asset(
    key_prefix=asset_prefix,
    ins={
        "catalog_as_dataframe": AssetIn(partition_mapping=needed_dataset_mapping),
    },
)
def filter_needed_catalog(context, needed_datasets, catalog_as_dataframe: pd.DataFrame) -> pd.DataFrame:

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


@multi_asset(
    ins={
        "individual_census_table": AssetIn(key_prefix=asset_prefix, partition_mapping=needed_dataset_mapping),
        # "individual_census_table": AssetIn(key_prefix=asset_prefix),
        "filter_needed_catalog": AssetIn(key_prefix=asset_prefix),
    },
    outs={
        "result_df": AssetOut(key_prefix=asset_prefix),
        "result_mmd": AssetOut(key_prefix=asset_prefix),
    },
    partitions_def=dataset_node_partition,
)
def get_enriched_tables(context, individual_census_table, filter_needed_catalog) -> tuple[pd.DataFrame, MetricMetadata]:

    ic(context)
    partition_keys = context.asset_partition_keys_for_input(input_name="individual_census_table")
    output_partition = context.asset_partition_key_for_output("result_df")
    ic(partition_keys)
    ic(len(partition_keys))
    ic(output_partition)
    ic(type(output_partition))

    if output_partition not in partition_keys: 
        err_msg = f"Requested partition {output_partition} not found in the subset of 'needed' partitions {partition_keys}"
        raise ValueError(err_msg)

    if output_partition not in individual_census_table:
        raise ValueError(f"Partition key {output_partition} not found in individual_census_table\n"
                         f"Available keys are {individual_census_table.keys()}")

    result_df = individual_census_table[output_partition]
    catalog_row =  filter_needed_catalog[filter_needed_catalog["node"].eq(output_partition)]
    ic(catalog_row)
    ic(type(catalog_row))
    catalog_row = catalog_row.to_dict(orient="index")[0]
    result_mmd =  census_table_metadata(context, catalog_row)

    # pivot_data(context, result_df, catalog_row)

    return result_df, result_mmd



# @asset(
#     key_prefix=asset_prefix,
#     partitions_def=dataset_node_partition,
# )
def pivot_data(
    context,
    census_table: pd.DataFrame,
    catalog_row
):

    # Check that the columns we need are present (currently failing on Windows) on CI for some unknown reason
    ic(census_table.columns)
    ic(catalog_row["source_column"])
    assert catalog_row["source_column"] in census_table.columns
    # assert "CD_AGE" in census_table.columns
    # assert "MS_POPULATION" in census_table.columns
    assert len(census_table) > 0

    ic(census_table.head())
    ic(catalog_row)

    node = catalog_row["node"]
    source_column = catalog_row["source_column"]
    metrics = derived_columns[derived_columns["node"].eq(node)]



    # # Drop all the columns we don't need
    # census_table = census_table[
    #     [
    #         "CD_REFNIS",  # keep
    #         # "CD_DSTR_REFNIS",   # drop
    #         # "CD_PROV_REFNIS",   # drop
    #         # "CD_RGN_REFNIS",    # drop
    #         "CD_SEX",  # keep
    #         # "CD_NATLTY",        # drop
    #         # "CD_CIV_STS",       # drop
    #         "CD_AGE",  # keep
    #         "MS_POPULATION",  # keep
    #         # "CD_YEAR",          # drop
    #     ]
    # ]

    # # Check that the columns we need are present (currently failing on Windows) on CI for some unknown reason
    # assert "CD_REFNIS" in census_table.columns
    # assert "CD_AGE" in census_table.columns
    # assert "MS_POPULATION" in census_table.columns
    # assert len(census_table) > 0

    new_table: pd.DataFrame = pd.DataFrame()

    # # Using HXL tags for variable names (https://hxlstandard.org/standard/1-1final/dictionary/#tag_population)
    # columns: dict[str, pd.Series[bool]] = {
    #     "population_children_age5_17": (census_table["CD_AGE"] >= 5) & (census_table["CD_AGE"] < 18),
    #     "population_infants_age0_4": (census_table["CD_AGE"] <= 4),
    #     "population_children_age0_17": (census_table["CD_AGE"] >= 0) & (census_table["CD_AGE"] < 18),
    #     "population_adults_f": (census_table["CD_AGE"] > 18) & (census_table["CD_SEX"] == "F"),
    #     "population_adults_m": (census_table["CD_AGE"] > 18) & (census_table["CD_SEX"] == "M"),
    #     "population_adults": (census_table["CD_AGE"] > 18),
    #     "population_ind": (census_table["CD_AGE"] >= 0),
    # }


    for _, col_name, group_by_column, filter in metrics.itertuples():
        new_col_def = {col_name: pd.NamedAgg(column=source_column, aggfunc="sum")}
        temp_table: pd.DataFrame = (
            census_table.loc[filter]
            .groupby(by=group_by_column, as_index=True)
            .agg(
                func=None,
                **new_col_def,  # type: ignore TODO, don't know why pyright is complaining here
            )
        )

        if len(new_table) == 0:
            new_table = temp_table
        else:
            new_table = new_table.merge(
                temp_table, left_index=True, right_index=True, how="inner"
            )

    new_table.set_index(group_by_column, inplace=True, drop=False)

    context.add_output_metadata(
        metadata={
            "num_records": len(new_table),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in new_table.columns.to_list()])
            ),
            "preview": MetadataValue.md(new_table.head().to_markdown()),
        }
    )

    return new_table