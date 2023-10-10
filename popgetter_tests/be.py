from popgetter.assets import be
import matplotlib.pyplot as plt

from dagster import (
    build_asset_context,
)
import pytest


def test_get_geometries():
    # Test the that the row count is correctly added to the metadata
    context = build_asset_context()

    # Check that the metadata is empty initially
    assert (
        (context.get_output_metadata(output_name="result") is None) |
        (context.get_output_metadata(output_name="result") == {})
    )

    # # Get the geometries
    stat_sectors = be.get_geometries(context)

    # Now check that the metadata has been updated
    metadata = context.get_output_metadata(output_name="result")
    assert metadata["num_records"] == 19795
