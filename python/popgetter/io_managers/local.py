from __future__ import annotations

import os

import geopandas as gpd
import pandas as pd
from dagster import OutputContext, MetadataValue
from upath import UPath

from . import TopLevelGeometryIOManager, TopLevelMetadataIOManager
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    SourceDataRelease,
    GeometryMetadata,
    MetricMetadata,
    metadata_to_dataframe,
)


class DagsterHomeMixin:
    dagster_home: str | None = os.getenv("DAGSTER_HOME")

    def get_base_path_local(self) -> UPath:
        if not self.dagster_home:
            raise ValueError("The DAGSTER_HOME environment variable must be set.")
        return UPath(self.dagster_home) / "cloud_outputs"


class LocalTopLevelMetadataIOManager(TopLevelMetadataIOManager, DagsterHomeMixin):
    def handle_output(
        self,
        context: OutputContext,
        obj: CountryMetadata | DataPublisher | SourceDataRelease,
    ) -> None:
        rel_path = self.get_relative_path(context, obj)
        full_path = self.get_base_path_local() / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        context.add_output_metadata(metadata={"parquet_path": str(full_path)})
        with full_path.open("wb") as file:
            file.write(self.to_binary(obj))


class LocalGeometryIOManager(TopLevelGeometryIOManager, DagsterHomeMixin):
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
        base_path = self.get_base_path_local()

        for obj_component in obj:
            geo_metadata, gdf, names_df = obj_component

            rel_paths = self.get_relative_paths(context, geo_metadata)
            full_paths = {
                key: base_path / rel_path for key, rel_path in rel_paths.items()
            }
            for path in full_paths.values():
                path.parent.mkdir(parents=True, exist_ok=True)

            output_metadata["flatgeobuf_paths"].append(str(full_paths["flatgeobuf"]))
            output_metadata["pmtiles_paths"].append(str(full_paths["pmtiles"]))
            output_metadata["geojsonseq_paths"].append(str(full_paths["geojsonseq"]))
            output_metadata["names_paths"].append(str(full_paths["names"]))

            gdf.to_file(full_paths["flatgeobuf"], driver="FlatGeobuf")
            gdf.to_file(full_paths["geojsonseq"], driver="GeoJSONSeq")
            # TODO: generate pmtiles
            names_df.to_parquet(full_paths["names"])

        # Metadata has to be serialised separately since they all get put in
        # the same path.
        metadata_df_filepath = base_path / self.get_relative_path_for_metadata(context)
        metadata_df = metadata_to_dataframe([md for md, _, _ in obj])
        metadata_df.to_parquet(metadata_df_filepath)

        context.add_output_metadata(
            metadata={
                "metadata_path": MetadataValue.md(str(metadata_df_filepath)),
                "metadata_preview": MetadataValue.md(metadata_df.head().to_markdown()),
                **output_metadata,
            }
        )
