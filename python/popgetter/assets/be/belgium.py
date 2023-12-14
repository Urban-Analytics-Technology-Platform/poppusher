from popgetter.metadata import CountryMetadata
from pathlib import Path

country : CountryMetadata = CountryMetadata(
        name_short_en="Belgium",
        name_official="Kingdom of Belgium",
        iso3="BEL",
        iso2="BE",
    )

WORKING_DIR = Path("belgium")
asset_prefix = "be"
