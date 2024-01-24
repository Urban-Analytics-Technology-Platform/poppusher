from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from dagster import (
    build_asset_context,
)

from popgetter.assets import demo_for_tech_talk


@pytest.fixture(scope="module")
def demo_sectors() -> gpd.GeoDataFrame:
    input_path = str(Path(__file__).parent / "demo_data" / "be_demo_sector.geojson")
    return gpd.read_file(input_path)


def test_aggregate_sectors_to_municipalities(demo_sectors):
    # Test the that the row count is correctly added to the metadata
    context = build_asset_context()

    actual_municipalities = (
        demo_for_tech_talk.census_geometry.aggregate_sectors_to_municipalities(
            context, demo_sectors
        )
    )

    expected_sector_row_count = 7
    expected_municipalities_row_count = 3

    assert len(demo_sectors) == expected_sector_row_count
    assert len(actual_municipalities) == expected_municipalities_row_count
    metadata = context.get_output_metadata(output_name="result")
    assert metadata["num_records"] == expected_municipalities_row_count
