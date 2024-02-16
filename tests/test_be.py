from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
import rdflib
from dagster import (
    build_asset_context,
    dynamic_partitioned_config,
)
from icecream import ic
from rdflib import Graph, URIRef
from rdflib.namespace import DCAT

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
    stat_sectors = be.census_geometry.sector_geometries(context)

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


@pytest.mark.skip(reason="Fix test_get_population_details_per_municipality first")
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


def test_demo_catalog(demo_catalog):
    # There are 10 datasets in the demo catalogue
    expected_length = 10
    actual_length = len(list(demo_catalog.objects(subject=be.census_tables.opendata_catalog_root, predicate=DCAT.dataset, unique=False)))

    assert actual_length == expected_length


def test_get_mmd_from_dataset_node(demo_catalog):
    # Get the metadata for a specific dataset in the demo catalogue:
    # https://statbel.fgov.be/node/4151 "Population by Statistical sector"
    mmd = be.census_tables.get_mmd_from_dataset_node(demo_catalog, dataset_node=URIRef("https://statbel.fgov.be/node/4151"))

    # Check that the right distribution_url has been selected
    #
    # This dataset has two distributions:
    # (xlsx):    <https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.xlsx#distribution4151>,
    # (txt/zip): <https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip#distribution4151> ;
    #
    # We expect the txt/zip version to be selected.
    expected_distribution_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip#distribution4151"
    wrong_distribution_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.xlsx#distribution4151"

    assert str(mmd.source_download_url) == expected_distribution_url
    assert str(mmd.source_download_url) != wrong_distribution_url

    # We expect the title to be in English (not any of the other available languages)
    title_english = "Population by Statistical sector"
    title_german = "Bevölkerung nach statistischen Sektoren"
    title_french = "Population par secteur statistique"
    title_dutch = "Bevolking per statistische sector"

    assert mmd.human_readable_name == title_english
    assert mmd.human_readable_name != title_german
    assert mmd.human_readable_name != title_french
    assert mmd.human_readable_name != title_dutch


@pytest.mark.skip(reason="Not implemented")
def test_filter_by_language(demo_catalog):
    # Test case
    # This dataset is only available in Dutch and French
    # https://statbel.fgov.be/node/2654
    assert False


def test_catalog_as_dataframe(demo_catalog):
    # Convert the demo catalog to a DataFrame
    with build_asset_context() as context:
        catalog_df = be.census_tables.catalog_as_dataframe(context, demo_catalog)

        # Check that the catalog has been converted to a DataFrame
        assert isinstance(catalog_df, pd.DataFrame)

        # Check that the DataFrame has the expected number of rows
        expected_number_of_datasets = 10
        assert len(catalog_df) == expected_number_of_datasets

        # Also check that the metadata has been updated
        metadata = context.get_output_metadata(output_name="result")
        assert metadata["num_records"] == expected_number_of_datasets


def test_purepath_suffix():
    # examples
    cases = [
        ("https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip#distribution4151", "zip"),
        ("https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.xlsx#distribution4151", "xlsx"),
        ("https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.txt#distribution4151", "txt"),
    ]

    