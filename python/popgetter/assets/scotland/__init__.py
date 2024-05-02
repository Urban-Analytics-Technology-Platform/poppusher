#!/usr/bin/python3
from __future__ import annotations

from datetime import date
from pathlib import Path

import requests
from dagster import (
    asset,
)

from popgetter.metadata import CountryMetadata, DataPublisher, SourceDataRelease

country: CountryMetadata = CountryMetadata(
    name_short_en="Scotland",
    name_official="Scotland",
    iso3="GBR",
    iso2="GB",
    iso3166_2="GB-SCT",
)

publisher: DataPublisher = DataPublisher(
    name="National Records of Scotland",
    url="https://www.nrscotland.gov.uk/",
    description="National Records of Scotland (NRS) is a Non-Ministerial Department of "
    "the Scottish Government. Our purpose is to collect, preserve and "
    "produce information about Scotland's people and history and make it "
    "available to inform current and future generations.",
    countries_of_interest=[country.id],
)


@asset()
def country_metadata() -> CountryMetadata:
    """Returns a CountryMetadata of metadata about the country."""
    return country


@asset()
def publisher_metadata():
    """Returns a DataPublisher of metadata about the publisher."""
    return publisher


# From: https://github.com/alan-turing-institute/microsimulation/blob/37ce2843f10b83a8e7a225c801cec83b85e6e0d0/microsimulation/common.py#L32
REQUIRED_TABLES = [
    "QS103SC",
    "QS104SC",
    "KS201SC",
    "DC1117SC",
    "DC2101SC",
    "DC6206SC",
    "LC1117SC",
]
REQUIRED_TABLES_REGEX = "|".join(REQUIRED_TABLES)
# Currently including only releases matching tables included
REQUIRED_RELEASES = ["3A", "3I", "2A", "3C"]
GENERAL_METHODS_URL = "https://www.scotlandscensus.gov.uk/media/jx2lz54n/scotland-s_census_2011_general_report.pdf"
CENSUS_REFERENCE_DATE = date(2011, 3, 27)
CENSUS_COLLECTION_DATE = date(2011, 3, 27)
CENSUS_EXPECT_NEXT_UPDATE = date(2022, 1, 1)

sources: dict[str, SourceDataRelease] = {
    "3A": SourceDataRelease(
        name="Census 2011: Release 3A",
        date_published=date(2014, 2, 27),
        reference_period_start=CENSUS_REFERENCE_DATE,
        reference_period_end=CENSUS_REFERENCE_DATE,
        collection_period_start=CENSUS_COLLECTION_DATE,
        collection_period_end=CENSUS_COLLECTION_DATE,
        expect_next_update=CENSUS_EXPECT_NEXT_UPDATE,
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3a",
        data_publisher_id=publisher.id,
        description="TBC",
        geography_file="TBC",
        geography_level="TBC",
        countries_of_interest=[country.id],
    ),
    "3I": SourceDataRelease(
        name="Census 2011: Release 3I",
        date_published=date(2014, 9, 24),
        reference_period_start=date(2015, 10, 22),
        reference_period_end=date(2015, 10, 22),
        collection_period_start=date(2011, 10, 22),
        collection_period_end=date(2011, 10, 22),
        expect_next_update=date(2022, 1, 1),
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-release-3i",
        data_publisher_id=publisher.id,
        description="TBC",
        geography_file="TBC",
        geography_level="TBC",
        countries_of_interest=[country.id],
    ),
    "2A": SourceDataRelease(
        name="Census 2011: Release 2A",
        date_published=date(2013, 9, 26),
        reference_period_start=date(2015, 10, 22),
        reference_period_end=date(2015, 10, 22),
        collection_period_start=date(2011, 10, 22),
        collection_period_end=date(2011, 10, 22),
        expect_next_update=date(2022, 1, 1),
        url="https://www.nrscotland.gov.uk/news/2013/census-2011-release-2a",
        data_publisher_id=publisher.id,
        description="TBC",
        geography_file="TBC",
        geography_level="TBC",
        countries_of_interest=[country.id],
    ),
    "3C": SourceDataRelease(
        name="Census 2011: Release 3C",
        date_published=date(2014, 4, 9),
        reference_period_start=date(2015, 10, 22),
        reference_period_end=date(2015, 10, 22),
        collection_period_start=date(2011, 10, 22),
        collection_period_end=date(2011, 10, 22),
        expect_next_update=date(2022, 1, 1),
        url="https://www.nrscotland.gov.uk/news/2014/census-2011-releases-2d-and-3c",
        data_publisher_id=publisher.id,
        description="TBC",
        geography_file="TBC",
        geography_level="TBC",
        countries_of_interest=[country.id],
    ),
}


# Move to tests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0"
}


def download_file(
    cache_dir: str,
    url: str,
    file_name: Path | None = None,
    headers: dict[str, str] = HEADERS,
) -> Path:
    """Downloads file checking first if exists in cache, returning file name."""
    file_name = Path(cache_dir) / url.split("/")[-1] if file_name is None else file_name
    if not Path(file_name).exists():
        r = requests.get(url, allow_redirects=True, headers=headers)
        with Path(file_name).open("wb") as fp:
            fp.write(r.content)
    return file_name
