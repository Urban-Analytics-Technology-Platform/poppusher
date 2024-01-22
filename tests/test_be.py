from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
import rdflib
from dagster import (
    build_asset_context,
)
from icecream import ic
from rdflib import Graph

from popgetter.assets import be


@pytest.fixture(scope="module")
def demo_sectors() -> gpd.GeoDataFrame:
    input_path = str(Path(__file__).parent / "demo_data" / "be_demo_sector.geojson")
    return gpd.read_file(input_path)


@pytest.fixture(scope="module")
def demo_catalog() -> gpd.GeoDataFrame:
    input_path = str(
        Path(__file__).parent / "demo_data" / "statbel_opendata_subset.ttl"
    )

    graph = Graph()
    graph.parse(input_path, format="ttl")

    return graph


def test_get_sector_geometries():
    # Test the that the row count is correctly added to the metadata
    context = build_asset_context()

    # Check that the metadata is empty initially
    assert (context.get_output_metadata(output_name="result") is None) | (
        context.get_output_metadata(output_name="result") == {}
    )

    # # Get the geometries
    stat_sectors = be.census_geometry.get_sector_geometries(context)

    expected_sector_row_count = 19795

    # Now check that the metadata has been updated
    metadata = context.get_output_metadata(output_name="result")
    assert len(stat_sectors) == expected_sector_row_count
    assert metadata["num_records"] == expected_sector_row_count


def test_aggregate_sectors_to_municipalities(demo_sectors):
    # Test the that the row count is correctly added to the metadata
    context = build_asset_context()

    actual_municipalities = be.census_geometry.aggregate_sectors_to_municipalities(
        context, demo_sectors
    )

    expected_sector_row_count = 7
    expected_municipalities_row_count = 3

    assert len(demo_sectors) == expected_sector_row_count
    assert len(actual_municipalities) == expected_municipalities_row_count
    metadata = context.get_output_metadata(output_name="result")
    assert metadata["num_records"] == expected_municipalities_row_count


def test_get_population_details_per_municipality():
    with build_asset_context() as muni_context:
        stat_muni = be.census_tables.get_population_details_per_municipality(
            muni_context
        )

    ic(len(stat_muni))
    ic(stat_muni.columns)

    assert len(stat_muni) > 0
    assert len(stat_muni.columns) > 0

    pytest.fail("Not complete")


@pytest.mark.skip(reason="Fix test_get_population_details_per_municipality first")
def test_pivot_population():
    # Test the that the row count is correctly added to the metadata
    # muni_context = build_asset_context()

    with build_asset_context() as muni_context:
        # Check that the metadata is empty initially
        assert (muni_context.get_output_metadata(output_name="result") is None) | (
            muni_context.get_output_metadata(output_name="result") == {}
        )

        # Get the geometries
        stat_muni = be.census_tables.get_population_details_per_municipality(
            muni_context
        )

    assert len(stat_muni) > 0
    ic(len(stat_muni))
    ic(stat_muni.head())

    # pivot_context = build_asset_context()

    with build_asset_context() as pivot_context:
        # Pivot the population
        pivoted = be.pivot_population(pivot_context, stat_muni)

    expected_number_of_municipalities = 581

    # Now check that the metadata has been updated
    metadata = pivot_context.get_output_metadata(output_name="result")
    assert len(pivoted) == expected_number_of_municipalities
    assert metadata["num_records"] == expected_number_of_municipalities


@pytest.mark.skip(reason="Not completed")
def test_get_opendata_table_list():
    # Statbel Open Data provide a sample subset of their catalogue here:
    # https://raw.githubusercontent.com/belgif/inspire-dcat/main/statbel_opendata_subset.ttl
    # A copy of this is in the demo_data folder:
    # `tests/demo_data/statbel_opendata_subset.ttl`

    context = build_asset_context()
    my_graph: rdflib.Graph = be.census_tables.get_opendata_table_list(context)

    # find_str = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2023.zip"
    # find_str = "node/4689"
    # find_str = "Population by place of residence, nationality, marital status, age and sex"
    # find_str = "NodeID4689"
    # find_str = "distribution4689"

    from rdflib.namespace import DC, DCTERMS
    from rdflib.term import URIRef

    print(DC.title)
    print(DCTERMS.title)

    print("--------------")
    my_subject = URIRef("https://statbel.fgov.be/node/4689")

    for p, o in my_graph.predicate_objects(subject=my_subject, unique=False):
        # print((p,o))
        print((p, o))
        if hasattr(o, "language"):
            print(o.language)
        else:
            print(f"no language for '{o}'")

        print("--------------")

    # for thing in my_graph.objects(subject=my_subject, predicate=DCTERMS.title, unique=False):
    #     print (thing)

    # for thing in my_graph.objects(subject=my_subject, predicate=DCAT.landingPage, unique=False):
    #     print (thing.)

    print("--------------")

    # for ns in my_graph.namespaces():
    #     print(ns)

    # print("--------------")

    # print(my_graph.n3())
    # TF_SOC_POP_STRUCT_2023

    # for p in g.predicates(unique=True):
    #     print(p)

    pytest.fail("Not implemented")


# @pytest.mark.skip(reason="Not completed")
def test_generate_metadata_from_table_list(demo_catalog):
    # Generate metadata from the demo catalogue
    context = build_asset_context()
    actual_mmd_list = be.census_tables.generate_metadata_from_table_list(
        context, demo_catalog
    )

    # There are 10 datasets in the demo catalogue
    expected_length = 10
    actual_length = len(actual_mmd_list)
    assert actual_length == expected_length

    # Check that the right distribution_url has been selected
    #
    # One of the datasets in then demo catalogue is:
    # https://statbel.fgov.be/node/4151 "Population by Statistical sector"
    # This has two distributions:
    #
    # (xlsx):    <https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.xlsx#distribution4151>,
    # (txt/zip): <https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip#distribution4151> ;
    #
    # We expect the txt/zip version to be selected.
    expected_distribution_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip#distribution4151"
    wrong_distribution_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.xlsx#distribution4151"

    # Exactly one of these
    assert (
        len(
            [
                mmd
                for mmd in actual_mmd_list
                if expected_distribution_url in mmd.source_download_url
            ]
        )
        == 1
    )
    # Exactly zero of these
    assert (
        len(
            [
                mmd
                for mmd in actual_mmd_list
                if wrong_distribution_url in mmd.source_download_url
            ]
        )
        == 0
    )
