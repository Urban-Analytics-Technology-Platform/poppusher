from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from dagster import (
    build_asset_context,
)
from icecream import ic
from rdflib import Graph
from rdflib.namespace import DCAT

from popgetter.assets import be


@pytest.fixture(scope="module")
def demo_sectors() -> gpd.GeoDataFrame:
    input_path = str(Path(__file__).parent / "demo_data" / "be_demo_sector.geojson")
    return gpd.read_file(input_path)


@pytest.fixture(scope="module")
def demo_catalog() -> Graph:
    input_path = str(
        Path(__file__).parent / "demo_data" / "statbel_opendata_subset.ttl"
    )

    graph = Graph()
    graph.parse(input_path, format="ttl")

    return graph


@pytest.fixture(scope="module")
def demo_catalog_df(demo_catalog) -> pd.DataFrame:
    context = build_asset_context()
    return be.census_tables.catalog_as_dataframe(context, demo_catalog)


@pytest.mark.skip(reason="No longer required")
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


@pytest.mark.skip(
    reason="Need to re-implement aggregate_sectors_to_municipalities to work with the sectors coming from then partitioned asset."
)
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
    actual_length = len(
        list(
            demo_catalog.objects(
                subject=be.census_tables.opendata_catalog_root,
                predicate=DCAT.dataset,
                unique=False,
            )
        )
    )

    assert actual_length == expected_length


def test_catalog_metadata_details(demo_catalog_df):
    # Get the metadata for a specific dataset in the demo catalogue:
    # https://statbel.fgov.be/node/4151 "Population by Statistical sector"
    # mmd = be.census_tables.get_mmd_from_dataset_node(
    #     demo_catalog, dataset_node=URIRef("https://statbel.fgov.be/node/4151")
    # )

    row = demo_catalog_df[
        demo_catalog_df["node"].eq("https://statbel.fgov.be/node/4151")
    ].to_dict(orient="records")[0]

    # Check that the right distribution_url has been selected
    #
    # This dataset has two distributions:
    # (xlsx):    <https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.xlsx#distribution4151>,
    # (txt/zip): <https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip#distribution4151> ;
    #
    # We expect the txt/zip version to be selected.
    expected_distribution_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip#distribution4151"
    wrong_distribution_url = "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.xlsx#distribution4151"

    assert row["source_download_url"] == expected_distribution_url
    assert row["source_download_url"] != wrong_distribution_url

    # We expect the title to be in English (not any of the other available languages)
    title_english = "Population by Statistical sector"
    title_german = "Bevölkerung nach statistischen Sektoren"
    title_french = "Population par secteur statistique"
    title_dutch = "Bevolking per statistische sector"

    assert row["human_readable_name"] == title_english
    assert row["human_readable_name"] != title_german
    assert row["human_readable_name"] != title_french
    assert row["human_readable_name"] != title_dutch


@pytest.mark.skip(reason="Not implemented")
def test_filter_by_language():
    # Test case
    # This dataset is only available in Dutch and French
    # https://statbel.fgov.be/node/2654
    pytest.fail("Not implemented")


def test_catalog_as_dataframe(demo_catalog_df):
    # Check that the catalog has been converted to a DataFrame
    assert isinstance(demo_catalog_df, pd.DataFrame)

    # Check that the DataFrame has the expected number of rows
    expected_number_of_datasets = 10
    assert len(demo_catalog_df) == expected_number_of_datasets

    # # Convert the demo catalog to a DataFrame
    # with build_asset_context() as context:
    #     catalog_df = be.census_tables.catalog_as_dataframe(context, demo_catalog_df)

    #     # Check that the catalog has been converted to a DataFrame
    #     assert isinstance(catalog_df, pd.DataFrame)

    #     # Check that the DataFrame has the expected number of rows
    #     expected_number_of_datasets = 10
    #     assert len(catalog_df) == expected_number_of_datasets

    #     # Also check that the metadata has been updated
    #     metadata = context.get_output_metadata(output_name="result")
    #     assert metadata["num_records"] == expected_number_of_datasets


@pytest.mark.skip(reason="Not implemented")
def test_purepath_suffix():
    # examples
    # cases = [
    #     (
    #         "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip#distribution4151",
    #         "zip",
    #     ),
    #     (
    #         "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.xlsx#distribution4151",
    #         "xlsx",
    #     ),
    #     (
    #         "https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.txt#distribution4151",
    #         "txt",
    #     ),
    # ]
    pytest.fail("Not implemented")


def test_filter_known_failing_datasets():
    mock_catalog = [
        "https://statbel.fgov.be/node/4796",  # from census_derived._needed_dataset_nodes
        "https://statbel.fgov.be/node/3961",  # Known failing dataset
        "https://statbel.fgov.be/node/595",  # Known failing dataset
        "https://statbel.fgov.be/en/node",  # Incomplete URL
        "2676",  # Known failing dataset node number
    ]

    expected_list = [
        "https://statbel.fgov.be/node/4796",
        "https://statbel.fgov.be/en/node",
        "2676",
    ]

    actual_list = be.census_tables.filter_known_failing_datasets(mock_catalog)

    assert mock_catalog != expected_list
    assert actual_list != mock_catalog
    assert actual_list == expected_list
