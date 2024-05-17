from __future__ import annotations

from functools import reduce

from dagster import (
    AssetSelection,
    DefaultSensorStatus,
    Output,
    RunRequest,
    StaticPartitionsDefinition,
    asset,
    multi_asset_sensor,
)


class CloudAssetSensor:
    """
    Class which scaffolds a cloud sensor. This class defines a publishing
    asset, which uses a custom IO manager to publish data either to Azure or
    local storage.

    The publishing asset in turn monitors a list of country-specific assets,
    which are responsible for generating the data that are to be published.

    Arguments
    ---------

    `asset_names_to_monitor`: list[str]
        A list of asset names in the country pipelines that we want to monitor.
        Each asset name should be a single string and should be prefixed with
        the asset group name and a forward slash (e.g. 'be/geometry').

    `io_manager_key`: str
        The key of the IO manager used for publishing. See the 'resources' and
        'resources_by_env' dicts in python/popgetter/__init__.py.

    `prefix`: str
        A string used to disambiguate the different assets / sensors that are
        being generated here, as Dagster does not allow duplicated names.

    `interval`: int
        The minimum interval at which the sensor should run, in seconds.
    """

    def __init__(
        self,
        asset_names_to_monitor: list[str],
        io_manager_key: str,
        prefix: str,
        interval: int,
    ):
        self.asset_names_to_monitor = asset_names_to_monitor
        self.io_manager_key = io_manager_key
        self.publishing_asset_name = f"publish_{prefix}"
        self.sensor_name = f"sensor_{prefix}"
        self.interval = interval

        self.partition_definition = StaticPartitionsDefinition(
            self.asset_names_to_monitor
        )
        self.assets_to_monitor = reduce(
            lambda x, y: x | y,
            [AssetSelection.keys(k) for k in self.asset_names_to_monitor],
        )

    def create_publishing_asset(self):
        @asset(
            name=self.publishing_asset_name,
            partitions_def=self.partition_definition,
            io_manager_key=self.io_manager_key,
        )
        def publish(context):
            from popgetter import defs as popgetter_defs

            # load_asset_value expects a list of strings
            output = popgetter_defs.load_asset_value(context.partition_key.split("/"))
            return Output(output)

        return publish

    def create_sensor(self):
        @multi_asset_sensor(
            monitored_assets=self.assets_to_monitor,
            request_assets=AssetSelection.keys(self.publishing_asset_name),
            name=self.sensor_name,
            minimum_interval_seconds=self.interval,
            default_status=DefaultSensorStatus.RUNNING,
        )
        def inner_sensor(context):
            asset_events = context.latest_materialization_records_by_key()
            for asset_key, execution_value in asset_events.items():
                # Assets which were materialised since the last time the sensor
                # was run will have non-None execution_values (it will be an
                # dagster.EventLogRecord class, which contains more information
                # if needed).
                if execution_value is not None:
                    yield RunRequest(
                        run_key=None,
                        partition_key="/".join(asset_key.path),
                    )
                    context.advance_cursor({asset_key: execution_value})

        return inner_sensor
