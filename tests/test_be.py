from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from dagster import (
    build_asset_context,
)
from rdflib import Graph

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
    stat_sectors = be.census.get_geometries(context)

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
    stat_muni = be.census.get_population_details_per_municipality(muni_context)

    pivot_context = build_asset_context()

    # Pivot the population
    pivoted = be.pivot_population(pivot_context, stat_muni)

    expected_number_of_municipalities = 581

    # Now check that the metadata has been updated
    metadata = pivot_context.get_output_metadata(output_name="result")
    assert len(pivoted) == expected_number_of_municipalities
    assert metadata["num_records"] == expected_number_of_municipalities


def test_get_opendata_table_list():
    context = build_asset_context()
    my_graph: Graph = be.census.get_opendata_table_list(context)

    find_str = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2023.zip"

    find_str = "node/4689"
    find_str = "NodeID4689"
    find_str = "distribution4689"

    # print("******** Subjects ********")
    # for s, p, o in g:
    #     if s.find(find_str) != -1:
    #         print(f"s={s}")

    # print("******** Predicate ********")
    # for s, p, o in g:
    #     if p.find(find_str) != -1:
    #         print(f"p={p}")

    # print("******** Object ********")
    # for s, p, o in g:
    #     if o.find(find_str) != -1:
    #         print(f"o={o}")

    for subject, predicate, object in my_graph:
        if (
            subject.find(find_str) != -1
            or predicate.find(find_str) != -1
            or object.find(find_str) != -1
        ):
            print(f"s={subject}, p = {predicate}, o={object}")
            print()

    # TF_SOC_POP_STRUCT_2023

    # for p in g.predicates(unique=True):
    #     print(p)

    pytest.fail("Not implemented")
