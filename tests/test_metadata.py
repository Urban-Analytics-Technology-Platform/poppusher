from __future__ import annotations

from datetime import date

import pytest

from popgetter.metadata import DataPublisher, SourceDataRelease


@pytest.mark.xfail()
def test_source_data_release_validation_reference():
    SourceDataRelease(
        name="Test Data Release",
        date_published=date(2021, 1, 1),
        reference_period_start=date(2020, 12, 31),
        reference_period_end=date(2020, 1, 1),
        collection_period_start=date(2020, 1, 1),
        collection_period_end=date(2020, 12, 31),
        expect_next_update=date(2022, 1, 1),
        url="https://example.com",
        data_publisher_id="test_publisher_id",
        description="This is a test data release",
        geography_file="test_geography_file",
        geography_level="test_geography_level",
    )


@pytest.mark.xfail()
def test_source_data_release_validation_collection():
    SourceDataRelease(
        name="Test Data Release",
        date_published=date(2021, 1, 1),
        reference_period_start=date(2020, 1, 1),
        reference_period_end=date(2020, 12, 31),
        collection_period_start=date(2020, 12, 31),
        collection_period_end=date(2020, 1, 1),
        expect_next_update=date(2022, 1, 1),
        url="https://example.com",
        data_publisher_id="test_publisher_id",
        description="This is a test data release",
        geography_file="test_geography_file",
        geography_level="test_geography_level",
    )


def test_source_data_release_hash():
    source_data_release = SourceDataRelease(
        name="Test Data Release",
        date_published=date(2021, 1, 1),
        reference_period_start=date(2020, 1, 1),
        reference_period_end=date(2020, 12, 31),
        collection_period_start=date(2020, 1, 1),
        collection_period_end=date(2020, 12, 31),
        expect_next_update=date(2022, 1, 1),
        url="https://example.com",
        data_publisher_id="test_publisher_id",
        description="This is a test data release",
        geography_file="test_geography_file",
        geography_level="test_geography_level",
    )
    assert (
        source_data_release.id
        == "15e6144c641c637247bde426fba653f209717799e41df6709a589bafbb4014c1"
    )

    source_data_release2 = SourceDataRelease(
        name="Test Data Release2",
        date_published=date(2021, 1, 1),
        reference_period_start=date(2020, 1, 1),
        reference_period_end=date(2020, 12, 31),
        collection_period_start=date(2020, 1, 1),
        collection_period_end=date(2020, 12, 31),
        expect_next_update=date(2022, 1, 1),
        url="https://example.com",
        data_publisher_id="test_publisher_id",
        description="This is a test data release",
        geography_file="test_geography_file",
        geography_level="test_geography_level",
    )
    assert source_data_release.id != source_data_release2.id


def test_data_publisher_hash():
    data_publisher = DataPublisher(
        name="Test Publisher",
        url="https://example.com",
        description="This is a test publisher",
        countries_of_interest=["GBR"],
    )
    assert (
        data_publisher.id
        == "0238fa7ccdc4b5095e62d088a0377bb83e40f62895071f2cc2a75333a98895af"
    )

    data_publisher2 = DataPublisher(
        name="Test Publisher 2",
        url="https://example.com",
        description="This is a test publisher",
        countries_of_interest=["GBR"],
    )
    assert data_publisher.id != data_publisher2.id
