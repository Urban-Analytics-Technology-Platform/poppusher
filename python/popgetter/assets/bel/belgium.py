from __future__ import annotations

from pathlib import Path

from popgetter.metadata import CountryMetadata

country: CountryMetadata = CountryMetadata(
    name_short_en="Belgium",
    name_official="Kingdom of Belgium",
    iso3="BEL",
    iso2="BE",
    iso3166_2=None,
)

WORKING_DIR = Path("belgium")
asset_prefix = "bel"