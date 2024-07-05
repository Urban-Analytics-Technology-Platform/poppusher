from __future__ import annotations

from pathlib import Path

import pandas as pd
from dagster import build_asset_context
from icecream import ic

from popgetter.assets.uk import england_wales_census as ew_census
from popgetter.metadata import MetricMetadata


def test_retrieve_table_description():
    example_url = "https://www.nomisweb.co.uk/datasets/c2021ts009"
    expected_desction_snipet = "estimates that classify usual residents in England and Wales by sex and single year of age"
    actual_description = ew_census._retrieve_table_description(example_url)

    assert (
        expected_desction_snipet in actual_description
    ), "The description of the table did not included the expected text"


def test_uk__derived_metrics():
    mmd = MetricMetadata(
        human_readable_name="test_human_readable_name",
        source_download_url="test_source_download_url",
        source_archive_file_path="test_source_archive_file_path",
        source_documentation_url="test_source_documentation_url",
        source_data_release_id="geography code",
        # TODO - this is a placeholder
        parent_metric_id="unknown_at_this_stage",
        potential_denominator_ids=None,
        parquet_margin_of_error_file=None,
        parquet_margin_of_error_column=None,
        parquet_column_name="unknown_parquet_column_name",
        # TODO - this is a placeholder
        metric_parquet_path="unknown",
        hxl_tag="hxl_tag_unknown",
        description="description",
        source_metric_id="source_table.hxltag",
    )

    # Get a context for testing
    context = build_asset_context(partition_key="ltla/TS009")

    # TODO, replace this with a proper fixture
    demo_census_source_table_path = (
        Path(__file__).parent / "demo_data" / "gbr_ew_census2021-ts009-ltla.csv"
    )
    source_df = pd.read_csv(demo_census_source_table_path)

    # Limit the test data to just Hartlepool and Middlesbrough
    source_df = source_df[source_df["geography code"].isin(["E06000001", "E06000002"])]

    ewc = ew_census.EnglandAndWales()
    actual_derived_metrics = ewc._derived_metrics(context, source_df, mmd)

    # ic(actual_derived_metrics)
    ic(actual_derived_metrics.metrics.head(1).T)

    # Test the results on Hartlepool
    hartlepool = actual_derived_metrics.metrics[
        actual_derived_metrics.metrics["geography code"] == "E06000001"
    ]

    # Manually calculated expected values
    assert hartlepool["children_5_17"].to_numpy()[0] == 14847
    assert hartlepool["infants_0_4"].to_numpy()[0] == 4981

    assert hartlepool["children_0_17"].to_numpy()[0] == 19828
    assert hartlepool["adults_f"].to_numpy()[0] == 37971
    assert hartlepool["adults_m"].to_numpy()[0] == 34548
    assert hartlepool["adults"].to_numpy()[0] == 72519
    assert hartlepool["individuals"].to_numpy()[0] == 92347
