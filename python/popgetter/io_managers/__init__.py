from __future__ import annotations

from typing import ClassVar

import geopandas as gpd
import pandas as pd
from dagster import InputContext, IOManager, OutputContext
from icecream import ic
from upath import UPath
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    SourceDataRelease,
    GeometryMetadata,
    MetricMetadata,
    metadata_to_dataframe,
)


class TopLevelMetadataIOManager(IOManager):
    def get_output_filename(
        self, obj: CountryMetadata | DataPublisher | SourceDataRelease
    ) -> str:
        if isinstance(obj, CountryMetadata):
            return "country_metadata.parquet"
        elif isinstance(obj, DataPublisher):
            return "data_publishers.parquet"
        elif isinstance(obj, SourceDataRelease):
            return "source_data_releases.parquet"
        else:
            err_msg = "This IO manager only accepts CountryMetadata, DataPublisher, and SourceDataRelease"
            raise ValueError(err_msg)

    def get_relative_path(
        self,
        context: OutputContext,
        obj: CountryMetadata | DataPublisher | SourceDataRelease,
    ) -> UPath:
        ic(context.partition_key)
        path_prefixes = list(context.partition_key.split("/"))[:-1]
        filename = self.get_output_filename(obj)
        return UPath("/".join([*path_prefixes, filename]))

    def to_binary(
        self, obj: CountryMetadata | DataPublisher | SourceDataRelease
    ) -> bytes:
        df = metadata_to_dataframe([obj])
        return df.to_parquet(None)

    def load_input(self, _context: InputContext) -> pd.DataFrame:
        err_msg = "This IOManager is only for writing outputs"
        raise RuntimeError(err_msg)


class TopLevelGeometryIOManager(IOManager):
    def get_relative_paths(
        self,
        context: OutputContext,
        geo_metadata: GeometryMetadata,
    ) -> dict[str, UPath]:
        filename_stem = geo_metadata.filename_stem
        asset_prefix = list(context.partition_key.split("/"))[:-1]  # e.g. ['be']
        return {
            "flatgeobuf": UPath(
                "/".join([*asset_prefix, "geometries", f"{filename_stem}.fgb"])
            ),
            "pmtiles": UPath(
                "/".join([*asset_prefix, "geometries", f"TODO_{filename_stem}.pmtiles"])
            ),
            "geojsonseq": UPath(
                "/".join([*asset_prefix, "geometries", f"{filename_stem}.geojsonseq"])
            ),
            "names": UPath(
                "/".join([*asset_prefix, "geometries", f"{filename_stem}.parquet"])
            ),
        }

    def get_relative_path_for_metadata(
        self,
        context: OutputContext,
    ) -> UPath:
        asset_prefix = list(context.partition_key.split("/"))[:-1]
        return UPath("/".join([*asset_prefix, "geometry_metadata.parquet"]))

    def load_input(self, _context: InputContext) -> pd.DataFrame:
        err_msg = "This IOManager is only for writing outputs"
        raise RuntimeError(err_msg)
