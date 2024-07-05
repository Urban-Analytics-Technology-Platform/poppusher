from __future__ import annotations

from datetime import date

import pytest

from popgetter.metadata import (
    COL,
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    SourceDataRelease,
)


def test_column_name_uniqueness():
    # Ensure that there are no duplicate column names in the output parquet, so
    # that we don't have issues when merging them in the full catalogue.
    assert len({x.value for x in COL}) == len(COL)


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
        geometry_metadata_id="test_geom_id",
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
        geometry_metadata_id="test_geom_id",
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
        geometry_metadata_id="test_geom_id",
    )
    assert (
        source_data_release.id
        == "9ec7e234d73664339e4c1f04bfa485dbb17e204dd72dc3ffbb9cab6870475597"
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
        geometry_metadata_id="test_geom_id",
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


def test_geometry_hash():
    country_metadata = CountryMetadata(
        name_short_en="United States",
        name_official="United States of America",
        iso2="US",
        iso3="USA",
        iso3166_2=None,
    )
    geometry_metadata = GeometryMetadata(
        country_metadata=country_metadata,
        validity_period_start=date(2021, 1, 1),
        validity_period_end=date(2021, 1, 1),
        level="tract",
        hxl_tag="tract",
    )
    assert (
        geometry_metadata.id
        == "082cfebd7348ca2d06353ff1d73e6096a60960f9795a26de54faeda777cd7f5d"
    )
    geometry_metadata1 = GeometryMetadata(
        country_metadata=country_metadata,
        validity_period_start=date(2021, 1, 1),
        validity_period_end=date(2021, 1, 1),
        level="tract",
        hxl_tag="tract",
    )
    geometry_metadata2 = GeometryMetadata(
        country_metadata=country_metadata,
        validity_period_start=date(2020, 1, 1),
        validity_period_end=date(2021, 1, 1),
        level="tract",
        hxl_tag="tract",
    )
    geometry_metadata3 = GeometryMetadata(
        country_metadata=country_metadata,
        validity_period_start=date(2021, 1, 1),
        validity_period_end=date(2021, 2, 1),
        level="tract",
        hxl_tag="tract",
    )
    geometry_metadata4 = GeometryMetadata(
        country_metadata=country_metadata,
        validity_period_start=date(2021, 1, 1),
        validity_period_end=date(2021, 1, 1),
        level="block_group",
        hxl_tag="tract",
    )
    geometry_metadata5 = GeometryMetadata(
        country_metadata=country_metadata,
        validity_period_start=date(2021, 1, 1),
        validity_period_end=date(2021, 1, 1),
        level="tract",
        hxl_tag="block_group",
    )
    assert geometry_metadata.id == geometry_metadata1.id
    assert geometry_metadata.id != geometry_metadata2.id
    assert geometry_metadata.id != geometry_metadata3.id
    assert geometry_metadata.id != geometry_metadata4.id
    assert geometry_metadata.id != geometry_metadata5.id
