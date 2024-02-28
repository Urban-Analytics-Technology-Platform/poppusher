#!/usr/bin/python3
from __future__ import annotations

from dagster import (
    asset,
)

from popgetter.metadata import (
    CountryMetadata,
)

from . import (
    scotland, 
)


# @asset(key_prefix=asset_prefix)
# def get_country_metadata() -> CountryMetadata:
#     """
#     Returns a CountryMetadata of metadata about the country.
#     """
#     return country
