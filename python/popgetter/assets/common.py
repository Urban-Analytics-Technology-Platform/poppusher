from __future__ import annotations

from abc import ABC, abstractmethod

import geopandas as gpd
import pandas as pd
from dagster import DynamicPartitionsDefinition, asset

from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
    GeometryMetadata,
    MetricMetadata,
    SourceDataRelease,
)


class CountryAssetOuputs(ABC):
    @abstractmethod
    def get_metadata_asset_keys(self) -> list[str]:
        ...

    @abstractmethod
    def get_geo_asset_keys(self) -> list[str]:
        ...

    @abstractmethod
    def get_metric_asset_keys(self) -> list[str]:
        ...


class Country(ABC):
    dataset_node_partition: DynamicPartitionsDefinition

    def create_catalog(self):
        @asset()
        def catalog(context):
            return self._catalog(context)

        return catalog

    @abstractmethod
    def _catalog(self, context) -> pd.DataFrame:
        ...

    def create_country_metadata(self):
        @asset()
        def country_metadata(context):
            return self._country_metadata(context)

        return country_metadata

    @abstractmethod
    def _country_metadata(self, context) -> CountryMetadata:
        ...

    def create_data_publisher(self):
        @asset
        def data_publisher(context, country_metadata: CountryMetadata):
            return self._data_publisher(context, country_metadata)

        return data_publisher

    @abstractmethod
    def _data_publisher(
        self, context, country_metdata: CountryMetadata
    ) -> DataPublisher:
        ...

    def create_geometry(self):
        @asset()
        def geometry(context):
            return self._geometry(context)

        return geometry

    @abstractmethod
    def _geometry(
        self, context
    ) -> list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]]:
        ...

    def create_source_data_releases(self):
        @asset()
        def source_data_releases(
            context,
            geometry: list[tuple[GeometryMetadata, gpd.GeoDataFrame, pd.DataFrame]],
            data_publisher: DataPublisher,
        ):
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
        @asset(partitions_def=self.dataset_node_partition)
        def census_tables(context, catalog):
            return self._census_tables(context, catalog)

        return census_tables

    @abstractmethod
    def _census_tables(self, context, catalog: pd.DataFrame) -> pd.DataFrame:
        ...

    def create_source_metric_metadata(self):
        @asset(partitions_def=self.dataset_node_partition)
        def source_metric_metadata(context, catalog, source_data_releases):
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
        @asset(partitions_def=self.dataset_node_partition)
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