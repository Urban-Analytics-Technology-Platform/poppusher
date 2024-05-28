from __future__ import annotations

from dagster import AssetsDefinition

from .sensor_class import CloudAssetSensor

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


def send_to_metadata_sensor(asset: AssetsDefinition):
    metadata_factory.monitored_asset_keys.append(asset.key)
    return asset


def send_to_geometry_sensor(asset: AssetsDefinition):
    geometry_factory.monitored_asset_keys.append(asset.key)
    return asset


def send_to_metrics_sensor(asset: AssetsDefinition):
    metrics_factory.monitored_asset_keys.append(asset.key)
    return asset
