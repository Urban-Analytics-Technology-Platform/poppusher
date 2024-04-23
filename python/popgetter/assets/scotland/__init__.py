#!/usr/bin/python3
from __future__ import annotations

from pathlib import Path

import requests
from dagster import (
    asset,
)

# from popgetter.assets.scotland import country
# from . import (
#     scotland,
# )
from popgetter.metadata import (
    CountryMetadata,
)

country: CountryMetadata = CountryMetadata(
    name_short_en="Scotland",
    name_official="Kingdom of Belgium",
    iso3="GBR",
    iso2="GB",
    iso3116_2="GB-SCT",
)

WORKING_DIR = Path("scotland")


@asset()
def get_country_metadata() -> CountryMetadata:
    """Returns a CountryMetadata of metadata about the country."""
    return country


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
