from __future__ import annotations

from .sensor_class import CloudAssetSensor

metadata_factory = CloudAssetSensor(
    asset_names_to_monitor=[
        "be/country_metadata",
        "be/data_publisher",
        "be/source_data_release_munip",
    ],
    io_manager_key="metadata_io_manager",
    prefix="metadata",
    interval=20,
)

metadata_sensor = metadata_factory.create_sensor()
metadata_asset = metadata_factory.create_publishing_asset()

geometry_factory = CloudAssetSensor(
    asset_names_to_monitor=[
        "be/geometry",
    ],
    io_manager_key="geometry_io_manager",
    prefix="geometry",
    interval=60,
)

geometry_sensor = geometry_factory.create_sensor()
geometry_asset = geometry_factory.create_publishing_asset()

metrics_factory = CloudAssetSensor(
    asset_names_to_monitor=[
        "be/metrics",
    ],
    io_manager_key="metrics_io_manager",
    prefix="metrics",
    interval=60,
)

metrics_sensor = metrics_factory.create_sensor()
metrics_asset = metrics_factory.create_publishing_asset()
