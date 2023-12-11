from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from dagster import (
    build_asset_context,
)

from popgetter.assets import be


@pytest.fixture(scope="module")
def demo_sectors() -> gpd.GeoDataFrame:
    input_path = str(Path(__file__).parent / "demo_data" / "be_demo_sector.geojson")
    return gpd.read_file(input_path)


def test_get_geometries():
    # Test the that the row count is correctly added to the metadata
    context = build_asset_context()

    # Check that the metadata is empty initially
    assert (context.get_output_metadata(output_name="result") is None) | (
        context.get_output_metadata(output_name="result") == {}
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


def test_pivot_population():
    # Test the that the row count is correctly added to the metadata
    muni_context = build_asset_context()

    # Check that the metadata is empty initially
    assert (muni_context.get_output_metadata(output_name="result") is None) | (
        muni_context.get_output_metadata(output_name="result") == {}
    )

    # Get the geometries
    stat_muni = be.get_population_details_per_municipality(muni_context)

    pivot_context = build_asset_context()

    # Pivot the population
    pivoted = be.pivot_population(pivot_context, stat_muni)

    expected_number_of_municipalities = 581

    # Now check that the metadata has been updated
    metadata = pivot_context.get_output_metadata(output_name="result")
    assert len(pivoted) == expected_number_of_municipalities
    assert metadata["num_records"] == expected_number_of_municipalities
