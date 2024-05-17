from __future__ import annotations

from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from dagster import InputContext, IOManager, MetadataValue, OutputContext
from upath import UPath

from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
    metadata_to_dataframe,
)


class PopgetterIOManager(IOManager):
    def get_base_path(self) -> UPath:
        raise NotImplementedError

    def handle_df(
        self, context: OutputContext, df: pd.DataFrame, full_path: UPath
    ) -> None:
        raise NotImplementedError

    def load_input(self, _context: InputContext) -> pd.DataFrame:
        err_msg = "This IOManager is only for writing outputs"
        raise RuntimeError(err_msg)


class MetadataIOManager(PopgetterIOManager):
    def get_output_filename(
        self, obj: CountryMetadata | DataPublisher | SourceDataRelease
    ) -> str:
        if isinstance(obj, CountryMetadata):
            return "country_metadata.parquet"
        if isinstance(obj, DataPublisher):
            return "data_publishers.parquet"
        if isinstance(obj, SourceDataRelease):
            return "source_data_releases.parquet"

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
        self, _context: OutputContext, _geo_df: gpd.GeoDataFrame, _full_path: UPath
    ) -> None:
        err_msg = "Pmtiles not currently implemented. You shouldn't be calling this."
        raise RuntimeError(err_msg)

    @dataclass
    class GeometryOutputPaths:
        flatgeobuf: UPath
        pmtiles: UPath
        geojsonseq: UPath
        names: UPath

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
            self.handle_df(context, names_df, full_paths.names)

            output_metadata["flatgeobuf_paths"].append(str(full_paths.flatgeobuf))
            output_metadata["pmtiles_paths"].append(str(full_paths.pmtiles))
            output_metadata["geojsonseq_paths"].append(str(full_paths.geojsonseq))
            output_metadata["names_paths"].append(str(full_paths.names))

        metadata_df_filepath = self.get_full_path_metadata(context)
        metadata_df = metadata_to_dataframe([md for md, _, _ in obj])
        self.handle_df(context, metadata_df, metadata_df_filepath)

        context.add_output_metadata(
            metadata={
                "metadata_path": str(metadata_df_filepath),
                "metadata_preview": MetadataValue.md(metadata_df.head().to_markdown()),
                **output_metadata,
            }
        )


class MetricsIOManager(PopgetterIOManager):
    def get_full_path_metadata(
        self,
        context: OutputContext,
    ) -> UPath:
        base_path = self.get_base_path()
        asset_prefix = list(context.partition_key.split("/"))[:-1]
        return base_path / UPath("/".join([*asset_prefix, "metric_metadata.parquet"]))

    def get_full_path_metrics(
        self,
        context: OutputContext,
        parquet_path: str,
    ) -> UPath:
        base_path = self.get_base_path()
        asset_prefix = list(context.partition_key.split("/"))[:-1]
        return base_path / UPath("/".join([*asset_prefix, "metrics", parquet_path]))

    def handle_output(
        self,
        context: OutputContext,
        obj: list[tuple[str, list[MetricMetadata], pd.DataFrame]],
    ) -> None:
        # Aggregate all the MetricMetadatas into a single dataframe, then
        # serialise
        all_metadatas_df = metadata_to_dataframe(
            [md for _, per_file_metadatas, _ in obj for md in per_file_metadatas]
        )
        metadata_df_filepath = self.get_full_path_metadata(context)
        self.handle_df(context, all_metadatas_df, metadata_df_filepath)

        # Write dataframes to the parquet files specified in the first element
        # of the tuple
        for rel_path, _, df in obj:
            full_path = self.get_full_path_metrics(context, rel_path)
            self.handle_df(context, df, full_path)

        # Add metadata
        context.add_output_metadata(
            metadata={
                "metric_parquet_paths": [
                    str(self.get_full_path_metrics(context, rel_path))
                    for rel_path, _, _ in obj
                ],
                "num_metrics": len(all_metadatas_df),
                "metric_human_readable_names": all_metadatas_df[
                    "human_readable_name"
                ].tolist(),
                "metadata_parquet_path": str(metadata_df_filepath),
                "metadata_preview": MetadataValue.md(
                    all_metadatas_df.head().to_markdown()
                ),
            }
        )
