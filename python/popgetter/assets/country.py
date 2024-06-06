from __future__ import annotations

from abc import ABC, abstractmethod

import geopandas as gpd
import pandas as pd
from dagster import AssetDep, DynamicPartitionsDefinition, asset

from popgetter.cloud_outputs import (
    send_to_geometry_sensor,
    send_to_metadata_sensor,
    send_to_metrics_sensor,
)
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
)


class Country(ABC):
    """
    A general class that can be implemented for a given country providing asset
    factories and abstract methods to provide a template for a given country.

    Attributes:
        key_prefix (str): the prefix for the asset keys (e.g. "be" for Belgium)
        dataset_node_partition (DynamicPartitionsDefinition): a dynamic partitions
            definition populated at runtime with a partition per census table.

    """

    key_prefix: ClassVar[str]
    partition_name: str
    dataset_node_partition: DynamicPartitionsDefinition

    def add_partition_keys(self, context, keys: list[str]):
        context.instance.add_dynamic_partitions(
            partitions_def_name=self.partition_name,
            partition_keys=keys,
        )

    def remove_all_partition_keys(self, context):
        for partition_key in context.instance.get_dynamic_partitions(self.partition_name):
            context.instance.delete_dynamic_partition(self.partition_name, partition_key)
    def __init__(self, key_prefix: str):
        self.partition_name = f"{self.key_prefix}_nodes"
        self.dataset_node_partition = DynamicPartitionsDefinition(name=self.partition_name)

    def create_catalog(self):
        """Creates an asset providing a census metedata catalog."""

        @asset(key_prefix=self.key_prefix)
        def catalog(context) -> pd.DataFrame:
            return self._catalog(context)

        return catalog

    @abstractmethod
    def _catalog(self, context) -> pd.DataFrame:
        ...

    def create_country_metadata(self):
        """Creates an asset providing the country metadata."""

        @send_to_metadata_sensor
        @asset(key_prefix=self.key_prefix)
        def country_metadata(context):
            return self._country_metadata(context)

        return country_metadata

    @abstractmethod
    def _country_metadata(self, context) -> CountryMetadata:
        ...

    def create_data_publisher(self):
        """Creates an asset providing the data publisher metadata."""

        @send_to_metadata_sensor
        @asset(key_prefix=self.key_prefix)
        def data_publisher(context, country_metadata: CountryMetadata):
            return self._data_publisher(context, country_metadata)

        return data_publisher

    @abstractmethod
    def _data_publisher(
        self, context, country_metdata: CountryMetadata
    ) -> DataPublisher:
        ...

    def create_geometry(self):
        """
        Creates an asset providing a list of geometries, metadata and names
        at different resolutions.
        """

        @send_to_geometry_sensor
        @asset(key_prefix=self.key_prefix)
        def geometry(context):
            return self._geometry(context)

        return geometry

    @abstractmethod
    def _geometry(
        self, context
    ) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
        ...

    def create_source_data_releases(self):
        """
        Creates an asset providing the corresponding source data release metadata for
        each geometry.
        """

        @send_to_metadata_sensor
        @asset(key_prefix=self.key_prefix)
        def source_data_releases(
            context,
            geometry: list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]],
            data_publisher: DataPublisher,
        ) -> dict[str, SourceDataRelease]:
            return self._source_data_releases(context, geometry, data_publisher)

        return source_data_releases

    @abstractmethod
    def _source_data_releases(
        self,
        context,
        geometry: list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]],
        data_publisher: DataPublisher,
        # TODO: consider version without inputs so only output type specified
        # **kwargs,
    ) -> dict[str, SourceDataRelease]:
        ...

    def create_census_tables(self):
        """
        Creates an asset providing each census table as a dataframe for each
        partition.
        """

        @asset(partitions_def=self.dataset_node_partition, key_prefix=self.key_prefix)
        def census_tables(context, catalog: pd.DataFrame) -> pd.DataFrame:
            return self._census_tables(context, catalog)

        return census_tables

    @abstractmethod
    def _census_tables(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        ...

    def create_source_metric_metadata(self):
        """
        Creates an asset providing the metadata required for downstream metric
        derivation.
        """

        @asset(partitions_def=self.dataset_node_partition, key_prefix=self.key_prefix)
        def source_metric_metadata(
            context, catalog, source_data_releases: dict[str, SourceDataRelease]
        ) -> MetricMetadata:
            return self._source_metric_metadata(context, catalog, source_data_releases)

        return source_metric_metadata

    @abstractmethod
    def _source_metric_metadata(
        self,
        context,
        catalog: pd.DataFrame,
        source_data_releases: dict[str, SourceDataRelease],
    ) -> MetricMetadata:
        ...

    def create_derived_metrics(self):
        """
        Creates an asset providing the metrics derived from the census tables and the
        corresponding source metric metadata.
        """

        @asset(partitions_def=self.dataset_node_partition, key_prefix=self.key_prefix)
        def derived_metrics(
            context,
            census_tables: pd.DataFrame,
            source_metric_metadata: MetricMetadata,
        ) -> tuple[list[MetricMetadata], pd.DataFrame]:
            return self._derived_metrics(context, census_tables, source_metric_metadata)

        return derived_metrics

    @abstractmethod
    def _derived_metrics(
        self,
        context,
        census_tables: pd.DataFrame,
        source_metric_metadata: MetricMetadata,
    ) -> tuple[list[MetricMetadata], pd.DataFrame]:
        ...

    def create_metrics(self):
        """
        Creates an asset combining all partitions across census tables into a combined
        list of metric data file names (for output), list of metadata and metric
        dataframe.
        """

        @send_to_metrics_sensor
        # Note: does not seem possible to specify a StaticPartition derived from a DynamicPartition:
        # See: https://discuss.dagster.io/t/16717119/i-want-to-be-able-to-populate-a-dagster-staticpartitionsdefi
        @asset(deps=[AssetDep("derived_metrics")], key_prefix=self.key_prefix)
        def metrics(
            context,
            catalog: pd.DataFrame,
        ) -> list[tuple[str, list[MetricMetadata], pd.DataFrame]]:
            return self._metrics(context, catalog)

        return metrics

    @abstractmethod
    def _metrics(
        self,
        context,
        catalog: pd.DataFrame,
    ) -> list[tuple[str, list[MetricMetadata], pd.DataFrame]]:
        ...