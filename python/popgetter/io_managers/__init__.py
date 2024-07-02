from __future__ import annotations

from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from dagster import InputContext, IOManager, MetadataValue, OutputContext
from iso639 import iter_langs
from upath import UPath

from popgetter.cloud_outputs import GeometryOutput, MetricsOutput
from popgetter.metadata import (
    COL,
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
    metadata_to_dataframe,
)

# Set of all valid ISO 639-3 codes. (They are all lowercase)
VALID_ISO639_3_CODES = {lang.pt3 for lang in iter_langs()}


class IOManagerError(Exception):
    pass


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
        self, obj_entry: CountryMetadata | DataPublisher | SourceDataRelease
    ) -> str:
        if isinstance(obj_entry, CountryMetadata):
            return "country_metadata.parquet"
        if isinstance(obj_entry, DataPublisher):
            return "data_publishers.parquet"
        if isinstance(obj_entry, SourceDataRelease):
            return "source_data_releases.parquet"

        err_msg = "This IO manager only accepts CountryMetadata, DataPublisher, and SourceDataRelease"
        raise ValueError(err_msg)

    def get_full_path(
        self,
        context: OutputContext,
        obj_entry: CountryMetadata | DataPublisher | SourceDataRelease,
    ) -> UPath:
        path_prefixes = list(context.partition_key.split("/"))[:-1]
        filename = self.get_output_filename(obj_entry)
        return self.get_base_path() / UPath("/".join([*path_prefixes, filename]))

    def handle_output(
        self,
        context: OutputContext,
        obj: (
            CountryMetadata
            | DataPublisher
            | SourceDataRelease
            | list[CountryMetadata]
            | list[DataPublisher]
            | list[SourceDataRelease]
            | dict[str, CountryMetadata]
            | dict[str, DataPublisher]
            | dict[str, SourceDataRelease]
        ),
    ):
        def all_same_class_and_accepted(objs) -> bool:
            return len(objs) > 0 and (
                all(isinstance(obj, CountryMetadata) for obj in objs)
                or all(isinstance(obj, DataPublisher) for obj in objs)
                or all(isinstance(obj, SourceDataRelease) for obj in objs)
            )

        if isinstance(obj, dict) and all_same_class_and_accepted(obj.values()):
            vals = list(obj.values())
        elif isinstance(obj, list) and all_same_class_and_accepted(obj):
            vals = obj
        elif isinstance(obj, CountryMetadata | DataPublisher | SourceDataRelease):
            vals = [obj]
        else:
            err_msg = (
                "MetadataIOManager only accepts assets which produce"
                " individual `CountryMetadata`, `DataPublisher`, or"
                " `SourceDataRelease`; or lists of them; or dictionaries"
                " with them as the values. If lists or dictionaries are"
                " passed, they must be nonempty, and all items must be"
                " of the same type."
            )
            raise ValueError(err_msg)

        full_path = self.get_full_path(context, vals[0])
        context.add_output_metadata(metadata={"parquet_path": str(full_path)})
        self.handle_df(context, metadata_to_dataframe(vals), full_path)


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
        geo_metadata: GeometryMetadata,
    ) -> GeometryOutputPaths:
        filepath_stem = geo_metadata.filepath_stem
        base_path = self.get_base_path()
        return self.GeometryOutputPaths(
            flatgeobuf=base_path / UPath(f"{filepath_stem}.fgb"),
            pmtiles=base_path / UPath(f"TODO_{filepath_stem}.pmtiles"),
            geojsonseq=base_path / UPath(f"{filepath_stem}.geojsonseq"),
            names=base_path / UPath(f"{filepath_stem}.parquet"),
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
        obj: list[GeometryOutput],
    ) -> None:
        output_metadata = {
            "flatgeobuf_paths": [],
            "pmtiles_paths": [],
            "geojsonseq_paths": [],
            "names_paths": [],
        }

        for output in obj:
            # Perform checks on input data
            if set(output.gdf.columns) != {COL.GEO_ID.value, "geometry"}:
                err_msg = (
                    f"The geodataframe passed to GeometryIOManager must"
                    f" have only two columns, '{COL.GEO_ID.value}' and"
                    f" 'geometry'."
                )
                raise ValueError(err_msg)
            names_cols = set(output.names_df.columns)
            if COL.GEO_ID.value not in names_cols:
                # Check if it's the index, reset index if so
                if output.names_df.index.name == COL.GEO_ID.value:
                    output.names_df = output.names_df.reset_index()
                    names_cols = set(output.names_df.columns)
                else:
                    err_msg = (
                        f"The dataframe of names passed to GeometryIOManager"
                        f" must have a '{COL.GEO_ID.value}' column."
                    )
                    raise ValueError(err_msg)
            other_names_cols = names_cols - {COL.GEO_ID.value}
            if len(other_names_cols) == 0:
                err_msg = (
                    f"The dataframe of names passed to GeometryIOManager"
                    f" must have at least one column other than"
                    f" '{COL.GEO_ID.value}'."
                )
                raise ValueError(err_msg)
            not_iso639_3_cols = [
                col for col in other_names_cols if col not in VALID_ISO639_3_CODES
            ]
            if len(not_iso639_3_cols) > 0:
                err_msg = (
                    f"The names of the column(s)"
                    f" {not_iso639_3_cols} in the dataframe of"
                    f" names passed to GeometryIOManager are not"
                    f" valid ISO 639-3 codes."
                )
                raise ValueError(err_msg)
            # Coerce columns to strings (pandas doesn't do this automatically)
            output.gdf[COL.GEO_ID.value] = output.gdf[COL.GEO_ID.value].astype("string")
            output.names_df = output.names_df.astype("string")

            full_paths = self.get_full_paths_geoms(output.metadata)

            self.handle_flatgeobuf(context, output.gdf, full_paths.flatgeobuf)
            self.handle_geojsonseq(context, output.gdf, full_paths.geojsonseq)
            # TODO self.handle_pmtiles(context, output.gdf, full_paths.pmtiles)
            self.handle_df(context, output.names_df, full_paths.names)

            output_metadata["flatgeobuf_paths"].append(str(full_paths.flatgeobuf))
            output_metadata["pmtiles_paths"].append(str(full_paths.pmtiles))
            output_metadata["geojsonseq_paths"].append(str(full_paths.geojsonseq))
            output_metadata["names_paths"].append(str(full_paths.names))

        metadata_df_filepath = self.get_full_path_metadata(context)
        metadata_df = metadata_to_dataframe([geo_output.metadata for geo_output in obj])
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
        parquet_path: str,
    ) -> UPath:
        base_path = self.get_base_path()
        return base_path / UPath(parquet_path)

    def handle_output(
        self,
        context: OutputContext,
        obj: list[MetricsOutput],
    ) -> None:
        # Perform checks on input data
        all_filepaths = []
        for output in obj:
            # Check GEO_ID col
            metrics_cols = set(output.metrics.columns)
            if COL.GEO_ID.value not in metrics_cols:
                # Check if it's the index, reset index if so
                if output.metrics.index.name == COL.GEO_ID.value:
                    output.metrics = output.metrics.reset_index()
                    metrics_cols = set(output.metrics.columns)
                else:
                    err_msg = (
                        f"The dataframe of metrics passed to MetricsIOManager"
                        f" must have a '{COL.GEO_ID.value}' column. It only has"
                        f" columns {metrics_cols}."
                    )
                    raise ValueError(err_msg)
            other_metrics_cols = metrics_cols - {COL.GEO_ID.value}
            # The data column names must match the metadata
            metadata_cols = {mmd.parquet_column_name for mmd in output.metadata}
            if other_metrics_cols != metadata_cols:
                err_msg = (
                    "The dataframe of metrics passed to MetricsIOManager"
                    " must have the same columns as the metadata"
                    " specifies. The metadata specifies columns"
                    f" {metadata_cols}, but the dataframe has columns"
                    f" {other_metrics_cols}."
                )
                raise ValueError(err_msg)
            # In each tuple, the list of MetricMetadata must all have the same
            # filepath, as the corresponding dataframe is saved to that path
            this_mmd_filepaths = {mmd.metric_parquet_path for mmd in output.metadata}
            if len(this_mmd_filepaths) != 1:
                err_msg = (
                    "The list of MetricMetadata in each tuple passed to"
                    " MetricsIOManager must all have the same"
                    " `metric_parquet_path`."
                )
                raise ValueError(err_msg)
            # Check that it's not already been used
            this_filepath = this_mmd_filepaths.pop()
            if this_filepath in all_filepaths:
                err_msg = (
                    "Each MetricsOutput passed to MetricsIOManager must have a"
                    f" unique `metric_parquet_path`. The path {this_filepath}"
                    " is repeated."
                )
            all_filepaths.append(this_filepath)
        # Convert GEO_ID cols to strings
        for output in obj:
            output.metrics[COL.GEO_ID.value] = output.metrics[COL.GEO_ID.value].astype(
                "string"
            )

        # Aggregate all the MetricMetadatas into a single dataframe, then
        # serialise
        all_metadatas_df = metadata_to_dataframe(
            [m for metrics_output in obj for m in metrics_output.metadata]
        )
        metadata_df_filepath = self.get_full_path_metadata(context)
        self.handle_df(context, all_metadatas_df, metadata_df_filepath)

        # Write dataframes to the parquet files specified in the first element
        # of the tuple
        for metrics_output in obj:
            rel_path = metrics_output.metadata[0].metric_parquet_path
            full_path = self.get_full_path_metrics(rel_path)
            self.handle_df(context, metrics_output.metrics, full_path)

        # Add metadata
        context.add_output_metadata(
            metadata={
                "metric_parquet_paths": all_filepaths,
                "num_metrics": len(all_metadatas_df),
                "metric_human_readable_names": all_metadatas_df[
                    COL.METRIC_HUMAN_READABLE_NAME.value
                ].tolist(),
                "metadata_parquet_path": str(metadata_df_filepath),
                "metadata_preview": MetadataValue.md(
                    all_metadatas_df.head().to_markdown()
                ),
            }
        )


class MetricsSingleIOManager(PopgetterIOManager):
    def get_full_path_metadata(
        self,
        context: OutputContext,
    ) -> UPath:
        base_path = self.get_base_path()
        asset_prefix = list(context.partition_key.split("/"))[:-1]
        return base_path / UPath("/".join([*asset_prefix, "metric_metadata.parquet"]))

    def get_full_path_metrics(
        self,
        parquet_path: str,
    ) -> UPath:
        base_path = self.get_base_path()
        return base_path / UPath(parquet_path)

    def handle_output(
        self,
        context: OutputContext,
        metrics_output: MetricsOutput,
    ) -> None:
        # Check if empty
        if metrics_output.metrics.shape == (0, 0):
            err_msg = (
                "The dataframe of metrics passed to MetricsIOManager"
                f" is empty for partition key: {context.partition_key}"
            )
            raise ValueError(err_msg)

        # Check GEO_ID col
        metrics_cols = set(metrics_output.metrics.columns)
        if "GEO_ID" not in metrics_cols:
            # Check if it's the index, reset index if so
            if metrics_output.metrics.index.name == "GEO_ID":
                metrics_output.metrics = metrics_output.metrics.reset_index()
                metrics_cols = set(metrics_output.metrics.columns)
            else:
                err_msg = (
                    "The dataframe of metrics passed to MetricsIOManager"
                    " must have a 'GEO_ID' column. It only has columns"
                    f" {metrics_cols}."
                )
                raise ValueError(err_msg)

        # In each tuple, the list of MetricMetadata must all have the same
        # filepath, as the corresponding dataframe is saved to that path
        this_mmd_filepaths = {
            mmd.metric_parquet_path for mmd in metrics_output.metadata
        }
        if len(this_mmd_filepaths) != 1:
            err_msg = (
                "The list of MetricMetadata in each tuple passed to"
                " MetricsIOManager must all have the same"
                " `metric_parquet_path`."
            )
            raise ValueError(err_msg)

        # Check that it's not already been used
        # TODO: this check is not possible for partitioned assets since the
        # data is confined to each partition
        this_filepath = this_mmd_filepaths.pop()

        # Convert GEO_ID cols to strings
        metrics_output.metrics["GEO_ID"] = metrics_output.metrics["GEO_ID"].astype(
            "string"
        )

        rel_path = metrics_output.metadata[0].metric_parquet_path
        full_path = self.get_full_path_metrics(rel_path)
        self.handle_df(context, metrics_output.metrics, full_path)

        # Add metadata
        context.add_output_metadata(
            metadata={
                "metric_parquet_path": this_filepath,
                "num_metrics": metrics_output.metrics.shape[1] - 1,
            }
        )


class MetricsMetdataIOManager(PopgetterIOManager):
    def get_full_path_metadata(
        self,
        context: OutputContext,
    ) -> UPath:
        base_path = self.get_base_path()
        asset_prefix = context.asset_key.to_user_string().split("/")[:-1]
        return base_path / UPath("/".join([*asset_prefix, "metric_metadata.parquet"]))

    def handle_output(
        self,
        context: OutputContext,
        obj: list[MetricMetadata],
    ) -> None:
        all_metadatas_df = metadata_to_dataframe(obj)
        metadata_df_filepath = self.get_full_path_metadata(context)
        self.handle_df(context, all_metadatas_df, metadata_df_filepath)

        context.add_output_metadata(
            metadata={
                "num_metrics": all_metadatas_df.shape[0],
                "metric_human_readable_names": all_metadatas_df[
                    "human_readable_name"
                ].tolist(),
                "metadata_parquet_path": str(metadata_df_filepath),
                "metadata_preview": MetadataValue.md(
                    all_metadatas_df.head().to_markdown()
                ),
            }
        )
