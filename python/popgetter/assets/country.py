from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

import pandas as pd
from dagster import (
    AssetIn,
    DynamicPartitionsDefinition,
    SpecificPartitionsPartitionMapping,
    asset,
)

from popgetter.cloud_outputs import (
    GeometryOutput,
    MetricsOutput,
    send_to_geometry_sensor,
    send_to_metadata_sensor,
    send_to_metrics_sensor,
)
from popgetter.metadata import (
    CountryMetadata,
    DataPublisher,
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

    def __init__(self):
        self.partition_name = f"{self.key_prefix}_nodes"
        self.dataset_node_partition = DynamicPartitionsDefinition(
            name=self.partition_name
        )

    def add_partition_keys(self, context, keys: list[str]):
        context.instance.add_dynamic_partitions(
            partitions_def_name=self.partition_name,
            partition_keys=keys,
        )

    def remove_all_partition_keys(self, context):
        for partition_key in context.instance.get_dynamic_partitions(
            self.partition_name
        ):
            context.instance.delete_dynamic_partition(
                self.partition_name, partition_key
            )

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
    def _geometry(self, context) -> list[GeometryOutput]:
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
            geometry: list[GeometryOutput],
            data_publisher: DataPublisher,
        ) -> dict[str, SourceDataRelease]:
            return self._source_data_releases(context, geometry, data_publisher)

        return source_data_releases

    @abstractmethod
    def _source_data_releases(
        self,
        context,
        geometry: list[GeometryOutput],
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
        ) -> MetricsOutput:
            return self._derived_metrics(context, census_tables, source_metric_metadata)

        return derived_metrics

    @abstractmethod
    def _derived_metrics(
        self,
        context,
        census_tables: pd.DataFrame,
        source_metric_metadata: MetricMetadata,
    ) -> MetricsOutput:
        ...

    def create_metrics(
        self,
        partitions_to_publish: list[str] | None = None,
    ):
        """
        Creates an asset combining all partitions across census tables into a combined
        list of metric data file names (for output), list of metadata and metric
        dataframe.
        """

        if partitions_to_publish is None:
            # Since this asset is unpartitioned, if the partition_mapping for
            # the upstream asset is not specified, Dagster assumes that this
            # asset depends on all upstream partitions.
            partition_kwargs = {}
        else:
            partition_kwargs = {
                "partition_mapping": SpecificPartitionsPartitionMapping(
                    partitions_to_publish
                )
            }

        @send_to_metrics_sensor
        @asset(
            key_prefix=self.key_prefix,
            ins={
                "derived_metrics": AssetIn(
                    key_prefix=self.key_prefix,
                    **partition_kwargs,  # pyright: ignore [reportArgumentType]
                )
            },
        )
        def metrics(
            context,
            # In principle, the derived_metrics should have the type:
            #    dict[str, MetricsOutput] | MetricsOutput
            # But Dagster doesn't like union types so we just use Any.
            derived_metrics,
        ) -> list[MetricsOutput]:
            # If the input asset has multiple partitions, Dagster returns a
            # dictionary of {partition_key: output_value}. However, if only one
            # partition is required, Dagster passes only the output value of
            # that partition, instead of a dictionary with one key. In this
            # case, we need to reconstruct a dictionary to pass it to the
            # underlying method which expects a dictionary.
            # See: https://github.com/dagster-io/dagster/issues/15538
            if partitions_to_publish is None:
                partition_names = context.instance.get_dynamic_partitions(
                    self.partition_name
                )
            else:
                partition_names = partitions_to_publish
            if len(partition_names) == 1:
                derived_metrics = {partition_names[0]: derived_metrics}

            return self._metrics(context, derived_metrics)

        return metrics

    def _metrics(
        self,
        context,
        derived_metrics: dict[str, MetricsOutput],
    ) -> list[MetricsOutput]:
        """
        Method which aggregates all the outputs of the `derived_metrics` asset.
        The default implementation simply combines each partition's output into
        a list (ignoring any partitions which have empty outputs). This is
        typically all that is required; however, this method can be overridden
        if different logic is required.
        """
        outputs = [
            output for output in derived_metrics.values() if len(output.metadata) > 0
        ]
        context.add_output_metadata(
            metadata={
                "num_metrics": sum(len(output.metadata) for output in outputs),
                "num_parquets": len(outputs),
            },
        )
        return outputs
