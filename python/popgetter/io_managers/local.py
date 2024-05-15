from __future__ import annotations

import os

import geopandas as gpd
import pandas as pd
from dagster import OutputContext
from upath import UPath

from . import GeoIOManager, PopgetterIOManager, TopLevelMetadataIOManager


class LocalMixin:
    dagster_home: str | None = os.getenv("DAGSTER_HOME")

    def get_base_path(self) -> UPath:
        if not self.dagster_home:
            err = "The DAGSTER_HOME environment variable must be set."
            raise ValueError(err)
        return UPath(self.dagster_home) / "cloud_outputs"

    def make_parent_dirs(self, full_path: UPath) -> None:
        full_path.parent.mkdir(parents=True, exist_ok=True)


class LocalTopLevelMetadataIOManager(
    LocalMixin, TopLevelMetadataIOManager, PopgetterIOManager
):
    def handle_df(
        self, _context: OutputContext, df: pd.DataFrame, full_path: UPath
    ) -> None:
        self.make_parent_dirs(full_path)
        df.to_parquet(full_path)


class LocalGeometryIOManager(LocalMixin, GeoIOManager, PopgetterIOManager):
    def handle_flatgeobuf(
        self, _context: OutputContext, geo_df: gpd.GeoDataFrame, full_path: UPath
    ) -> None:
        self.make_parent_dirs(full_path)
        geo_df.to_file(full_path, driver="FlatGeobuf")

    def handle_geojsonseq(
        self, _context: OutputContext, geo_df: gpd.GeoDataFrame, full_path: UPath
    ) -> None:
        self.make_parent_dirs(full_path)
        geo_df.to_file(full_path, driver="GeoJSONSeq")

    def handle_names(
        self, _context: OutputContext, names_df: pd.DataFrame, full_path: UPath
    ) -> None:
        self.make_parent_dirs(full_path)
        names_df.to_parquet(full_path)

    def handle_geo_metadata(
        self, _context: OutputContext, geo_metadata_df: pd.DataFrame, full_path: UPath
    ) -> None:
        self.make_parent_dirs(full_path)
        geo_metadata_df.to_parquet(full_path)
