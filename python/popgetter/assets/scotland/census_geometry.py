from __future__ import annotations

import geopandas as gpd
from dagster import asset

from popgetter.assets.scotland import download_file

from .scotland import URL_SHAPEFILE, add_metadata, cache_dir


@asset
def geometry(context, oa_dz_iz_2011_lookup) -> gpd.GeoDataFrame:
    """Gets the shape file for OA11 resolution."""
    file_name = download_file(cache_dir, URL_SHAPEFILE)
    geo = gpd.read_file(f"zip://{file_name}")
    add_metadata(context, geo, "Geometry file")
    return geo[geo["geo_code"].isin(oa_dz_iz_2011_lookup["OutputArea2011Code"])]
