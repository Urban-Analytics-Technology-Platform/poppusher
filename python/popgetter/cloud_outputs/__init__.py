from __future__ import annotations

from .sensor_class import CloudAssetSensor

from dagster import AssetsDefinition


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


def publish_metadata(asset: AssetsDefinition):
    metadata_factory.monitored_asset_keys.append(asset.key)
    return asset


def publish_geometries(asset: AssetsDefinition):
    geometry_factory.monitored_asset_keys.append(asset.key)
    return asset


def publish_metrics(asset: AssetsDefinition):
    metrics_factory.monitored_asset_keys.append(asset.key)
    return asset
