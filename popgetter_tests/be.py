from popgetter.assets import be
import matplotlib.pyplot as plt
import geopandas as gpd

from dagster import (
    build_asset_context,
)
import pytest



@pytest.fixture(scope="module")
def demo_sectors() -> gpd.GeoDataFrame:
    return gpd.read_file("popgetter_tests/demo_data/be_demo_sector.geojson")


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

    expected_sector_row_count = 19795

    # Now check that the metadata has been updated
    metadata = context.get_output_metadata(output_name="result")
    assert len(stat_sectors) == expected_sector_row_count
    assert metadata["num_records"] == expected_sector_row_count

def test_aggregate_sectors_to_municipalities(demo_sectors):
    # Test the that the row count is correctly added to the metadata
    context = build_asset_context()

    actual_munis = be.aggregate_sectors_to_municipalities(context, demo_sectors)

    expected_sector_row_count = 7
    expected_munis_row_count = 3

    assert len(demo_sectors) == expected_sector_row_count
    assert len(actual_munis) == expected_munis_row_count
    metadata = context.get_output_metadata(output_name="result")
    assert metadata["num_records"] == expected_munis_row_count


