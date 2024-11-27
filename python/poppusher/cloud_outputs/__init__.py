from __future__ import annotations

from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from dagster import AssetsDefinition

from poppusher.metadata import GeometryMetadata, MetricMetadata

from .sensor_class import CloudAssetSensor


@dataclass
class GeometryOutput:
    """This class conceptualises the expected output types of a geometry
    asset. Specifically, the asset marked with `@send_to_geometry_sensor` has to
    output a list of GeometryOutput objects (one per geometry level / year)."""

    metadata: GeometryMetadata
    gdf: gpd.GeoDataFrame
    names_df: pd.DataFrame


@dataclass
class MetricsOutput:
    """This class conceptualises the expected output types of a metrics
    asset. Specifically, the asset marked with `@send_to_metrics_sensor` has to
    output a list of MetricsOutput objects (one per parquet file; but each
    MetricsOutput object may correspond to multiple metrics that are serialised
    to the same parquet file)."""

    metadata: list[MetricMetadata]
    metrics: pd.DataFrame


metadata_factory = CloudAssetSensor(
    io_manager_key="metadata_io_manager",
    prefix="metadata",
    interval=20,
)

metadata_sensor = metadata_factory.create_sensor()
metadata_asset = metadata_factory.create_publishing_asset()

geometry_factory = CloudAssetSensor(
    io_manager_key="geometry_io_manager",
    prefix="geometry",
    interval=60,
)

geometry_sensor = geometry_factory.create_sensor()
geometry_asset = geometry_factory.create_publishing_asset()


metrics_factory = CloudAssetSensor(
    io_manager_key="metrics_io_manager",
    prefix="metrics",
    interval=60,
)

metrics_sensor = metrics_factory.create_sensor()
metrics_asset = metrics_factory.create_publishing_asset()

metrics_partitioned_factory = CloudAssetSensor(
    io_manager_key="metrics_partitioned_io_manager",
    prefix="metrics_partitioned",
    interval=60,
)

# TODO: commented out until implemented for partitioned assets
# metrics_partitioned_sensor = metrics_partitioned_factory.create_sensor()
# metrics_partitioned_asset = metrics_partitioned_factory.create_publishing_asset()

metrics_metadata_factory = CloudAssetSensor(
    io_manager_key="metrics_metadata_io_manager",
    prefix="metrics_metadata",
    interval=60,
)

metrics_metadata_sensor = metrics_metadata_factory.create_sensor()
metrics_metadata_asset = metrics_metadata_factory.create_publishing_asset()


def send_to_metadata_sensor(asset: AssetsDefinition):
    metadata_factory.monitored_asset_keys.append(asset.key)
    return asset


def send_to_geometry_sensor(asset: AssetsDefinition):
    geometry_factory.monitored_asset_keys.append(asset.key)
    return asset


def send_to_metrics_sensor(asset: AssetsDefinition):
    metrics_factory.monitored_asset_keys.append(asset.key)
    return asset


# TODO: need to implement handling for partitions for this sensor
def send_to_metrics_partitioned_sensor(asset: AssetsDefinition):
    # TODO: add partition key here
    metrics_partitioned_factory.monitored_asset_keys.append(asset.key)
    return asset


def send_to_metrics_metadata_sensor(asset: AssetsDefinition):
    metrics_metadata_factory.monitored_asset_keys.append(asset.key)
    return asset
