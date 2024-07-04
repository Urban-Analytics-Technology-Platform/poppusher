from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from dagster import build_asset_context
from icecream import ic

from popgetter.assets.uk import england_wales_census as ew_census


def test_retrieve_table_description():
    example_url = "https://www.nomisweb.co.uk/datasets/c2021ts009"
    expected_desction_snipet = "estimates that classify usual residents in England and Wales by sex and single year of age"
    actual_description = ew_census._retrieve_table_description(example_url)

    assert (
        expected_desction_snipet in actual_description
    ), "The description of the table did not included the expected text"


def test_uk__derived_metrics():
    # Get a context for testing
    context = build_asset_context()

    # TODO, replace this with a proper fixture
    demo_census_source_table_path = (
        Path(__file__).parent / "demo_data" / "gbr_ew_census2021-ts009-ltla.csv"
    )
    source_df = pd.read_csv(demo_census_source_table_path)
    ic(source_df.head())

    ewc = ew_census.EnglandAndWales()
    _actual_derived_metrics = ewc._derived_metrics(context, source_df, None)

    pytest.fail("Test not implemented")
