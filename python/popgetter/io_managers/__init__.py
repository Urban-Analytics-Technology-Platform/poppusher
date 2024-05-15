from __future__ import annotations

from typing import ClassVar

import geopandas as gpd
import pandas as pd
from dagster import InputContext, IOManager, OutputContext
from icecream import ic
from upath import UPath


class TopLevelMetadataIOManager(IOManager):
    output_filenames: ClassVar[dict[str, str]] = {
        "country_metadata": "country_metadata.parquet",
        "data_publisher": "data_publishers.parquet",
        "source_data_release": "source_data_releases.parquet",
    }

    def get_relative_path(self, context: OutputContext) -> UPath:
        try:
            ic(context.asset_key.path)
            path_components = list(context.asset_key.path)
            path_components[-1] = self.output_filenames[path_components[-1]]
            return UPath("/".join(path_components))
        except KeyError as err:
            err_msg = f"Only the asset keys {','.join(self.output_filenames.keys())} are compatible with this"
            raise ValueError(err_msg) from err

    def to_binary(self, obj: pd.DataFrame) -> bytes:
        return obj.to_parquet(None)

    def load_input(self, _context: InputContext) -> pd.DataFrame:
        err_msg = "This IOManager is only for writing outputs"
        raise RuntimeError(err_msg)


class TopLevelGeometryIOManager(IOManager):
    def get_relative_paths(
        self,
        context: OutputContext,
        obj: tuple[pd.DataFrame, gpd.GeoDataFrame, pd.DataFrame],
    ) -> dict[str, str]:
        filename_stem = obj[0].iloc[0]["filename_stem"]
        asset_prefix = list(context.asset_key.path[:-1])  # e.g. ['be']
        return {
            "metadata": "/".join([*asset_prefix, "geometry_metadata.parquet"]),
            "flatgeobuf": "/".join(
                [*asset_prefix, "geometries", f"{filename_stem}.fgb"]
            ),
            "pmtiles": "/".join(
                [*asset_prefix, "geometries", f"{filename_stem}.pmtiles"]
            ),
            "geojsonseq": "/".join(
                [*asset_prefix, "geometries", f"{filename_stem}.geojsonseq"]
            ),
            "names": "/".join(
                [*asset_prefix, "geometries", f"{filename_stem}.parquet"]
            ),
        }

    def load_input(self, _context: InputContext) -> pd.DataFrame:
        err_msg = "This IOManager is only for writing outputs"
        raise RuntimeError(err_msg)
