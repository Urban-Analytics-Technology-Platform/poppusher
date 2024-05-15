from __future__ import annotations

from typing import ClassVar

import geopandas as gpd
import pandas as pd
from dagster import InputContext, IOManager, OutputContext, MetadataValue
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
from dataclasses import dataclass


class PopgetterIOManager(IOManager):
    def get_base_path(self) -> UPath:
        raise NotImplementedError

    def load_input(self, _context: InputContext) -> pd.DataFrame:
        err_msg = "This IOManager is only for writing outputs"
        raise RuntimeError(err_msg)


class TopLevelMetadataIOManager(PopgetterIOManager):
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

    def get_full_path(
        self,
        context: OutputContext,
        obj: CountryMetadata | DataPublisher | SourceDataRelease,
    ) -> UPath:
        path_prefixes = list(context.partition_key.split("/"))[:-1]
        filename = self.get_output_filename(obj)
        return self.get_base_path() / UPath("/".join([*path_prefixes, filename]))

    def handle_df(self, context: OutputContext, df: pd.DataFrame, full_path: UPath) -> None:
        raise NotImplementedError

    def handle_output(
        self,
        context: OutputContext,
        obj: CountryMetadata | DataPublisher | SourceDataRelease,
    ):
        full_path = self.get_full_path(context, obj)
        context.add_output_metadata(metadata={"parquet_path": str(full_path)})
        self.handle_df(context, metadata_to_dataframe([obj]), full_path)


class GeoIOManager(PopgetterIOManager):
    def handle_flatgeobuf(
        self, context: OutputContext, geo_df: gpd.GeoDataFrame, full_path: UPath
    ) -> None:
        raise NotImplementedError

    def handle_geojsonseq(
        self, context: OutputContext, geo_df: gpd.GeoDataFrame, full_path: UPath
    ) -> None:
        raise NotImplementedError

    def handle_pmtiles(
        self, context: OutputContext, geo_df: gpd.GeoDataFrame, full_path: UPath
    ) -> None:
        raise RuntimeError("Pmtiles not currently implemented")

    def handle_names(
        self, context: OutputContext, names_df: pd.DataFrame, full_path: UPath
    ) -> None:
        raise NotImplementedError

    def handle_geo_metadata(
        self, context: OutputContext, geo_metadata_df: pd.DataFrame, full_path: UPath
    ) -> None:
        raise NotImplementedError

    @dataclass
    class GeometryOutputPaths:
        flatgeobuf: str
        pmtiles: str
        geojsonseq: str
        names: str

    def get_full_paths_geoms(
        self,
        context: OutputContext,
        geo_metadata: GeometryMetadata,
    ) -> GeometryOutputPaths:
        filename_stem = geo_metadata.filename_stem
        asset_prefix = list(context.partition_key.split("/"))[:-1]  # e.g. ['be']
        base_path = self.get_base_path()
        return self.GeometryOutputPaths(
            flatgeobuf=base_path
            / UPath("/".join([*asset_prefix, "geometries", f"{filename_stem}.fgb"])),
            pmtiles=base_path
            / UPath(
                "/".join([*asset_prefix, "geometries", f"TODO_{filename_stem}.pmtiles"])
            ),
            geojsonseq=base_path
            / UPath(
                "/".join([*asset_prefix, "geometries", f"{filename_stem}.geojsonseq"])
            ),
            names=base_path
            / UPath(
                "/".join([*asset_prefix, "geometries", f"{filename_stem}.parquet"])
            ),
        )

    def get_full_path_metadata(
        self,
        context: OutputContext,
    ) -> UPath:
        base_path = self.get_base_path()
        asset_prefix = list(context.partition_key.split("/"))[:-1]
        return base_path / UPath("/".join([*asset_prefix, "geometry_metadata.parquet"]))

    def handle_output(
        self,
        context: OutputContext,
        obj: list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]],
    ) -> None:
        output_metadata = {
            "flatgeobuf_paths": [],
            "pmtiles_paths": [],
            "geojsonseq_paths": [],
            "names_paths": [],
        }

        for geo_metadata, gdf, names_df in obj:
            full_paths = self.get_full_paths_geoms(context, geo_metadata)

            self.handle_flatgeobuf(context, gdf, full_paths.flatgeobuf)
            self.handle_geojsonseq(context, gdf, full_paths.geojsonseq)
            # TODO self.handle_pmtiles(context, gdf, full_paths.pmtiles)
            self.handle_names(context, names_df, full_paths.names)

            output_metadata["flatgeobuf_paths"].append(str(full_paths.flatgeobuf))
            output_metadata["pmtiles_paths"].append(str(full_paths.pmtiles))
            output_metadata["geojsonseq_paths"].append(str(full_paths.geojsonseq))
            output_metadata["names_paths"].append(str(full_paths.names))

        metadata_df_filepath = self.get_full_path_metadata(context)
        metadata_df = metadata_to_dataframe([md for md, _, _ in obj])
        self.handle_geo_metadata(context, metadata_df, metadata_df_filepath)

        context.add_output_metadata(
            metadata={
                "metadata_path": str(metadata_df_filepath),
                "metadata_preview": MetadataValue.md(metadata_df.head().to_markdown()),
                **output_metadata,
            }
        )
