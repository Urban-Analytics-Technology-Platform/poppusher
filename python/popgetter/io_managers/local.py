from __future__ import annotations

import os

import pandas as pd
from dagster import OutputContext
from upath import UPath

from . import TopLevelMetadataIOManager, TopLevelGeometryIOManager


class DagsterHomeMixin:
    dagster_home: str | None = os.getenv("DAGSTER_HOME")

    def get_base_path_local(self) -> UPath:
        if not self.dagster_home:
            raise ValueError("The DAGSTER_HOME environment variable must be set.")
        return UPath(self.dagster_home) / "cloud_outputs"


class LocalTopLevelMetadataIOManager(TopLevelMetadataIOManager, DagsterHomeMixin):
    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        rel_path = self.get_relative_path(context)
        full_path = self.get_base_path_local() / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        context.add_output_metadata(metadata={"parquet_path": str(full_path)})
        with full_path.open("wb") as file:
            file.write(self.to_binary(obj))


class LocalGeometryIOManager(TopLevelGeometryIOManager, DagsterHomeMixin):
    def handle_output(
        self,
        context: OutputContext,
        obj: tuple[pd.DataFrame, gpd.GeoDataFrame, pd.DataFrame],
    ) -> None:
        rel_paths = self.get_relative_paths(context, obj)
        base_path = self.get_base_path_local()
        full_paths = {key: base_path / rel_path for key, rel_path in rel_paths.items()}
        for path in full_paths.values():
            path.parent.mkdir(parents=True, exist_ok=True)
        context.add_output_metadata(
            metadata={
                "geometry_metadata_path": str(full_paths["metadata"]),
                "flatgeobuf_path": str(full_paths["flatgeobuf"]),
                "pmtiles_path": str(full_paths["pmtiles"]),
                "geojsonseq_path": str(full_paths["geojsonseq"]),
                "names_path": str(full_paths["names"]),
            }
        )

        metadata_df, gdf, names_df = obj
        metadata_df.to_parquet(full_paths["metadata"])
        gdf.to_file(full_paths["flatgeobuf"], driver="FlatGeobuf")
        gdf.to_file(full_paths["geojsonseq"], driver="GeoJSONSeq")
        # TODO: generate pmtiles
        names_df.to_parquet(full_paths["names"])
