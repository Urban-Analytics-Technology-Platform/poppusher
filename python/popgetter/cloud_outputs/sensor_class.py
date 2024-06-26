from __future__ import annotations

from dagster import (
    AssetKey,
    AssetSelection,
    DagsterInstance,
    DefaultSensorStatus,
    DynamicPartitionsDefinition,
    Output,
    RunRequest,
    asset,
    multi_asset_sensor,
)
from dagster._core.errors import DagsterHomeNotSetError


class CloudAssetSensor:
    """
    Class which scaffolds a cloud sensor. This class defines a publishing
    asset, which uses a custom IO manager to publish data either to Azure or
    local storage.

    The publishing asset in turn monitors a list of country-specific assets,
    which are responsible for generating the data that are to be published.

    Arguments
    ---------
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
        io_manager_key: str,
        prefix: str,
        interval: int,
    ):
        self.io_manager_key = io_manager_key
        self.publishing_asset_name = f"publish_{prefix}"
        self.sensor_name = f"sensor_{prefix}"
        self.interval = interval
        self.monitored_asset_keys: list[AssetKey] = []

        self.partition_definition_name = f"{prefix}_partitions"
        self.partition_definition = DynamicPartitionsDefinition(
            name=self.partition_definition_name
        )
        # Upon initialisation, remove all partitions so that the web UI doesn't
        # show any stray partitions left over from old Dagster launches (this
        # can happen when e.g. switching between different git branches)
        try:
            with DagsterInstance.get() as instance:
                for partition_key in instance.get_dynamic_partitions(
                    self.partition_definition_name
                ):
                    instance.delete_dynamic_partition(
                        self.partition_definition_name, partition_key
                    )
        except DagsterHomeNotSetError:
            pass

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
            # Because the list of monitored assets is dynamically generated
            # using self.monitored_asset_keys, we don't pass it here.
            monitored_assets=[],
            request_assets=AssetSelection.keys(self.publishing_asset_name),
            name=self.sensor_name,
            minimum_interval_seconds=self.interval,
            default_status=DefaultSensorStatus.RUNNING,
        )
        def inner_sensor(context):
            # Get info about the latest materialisations of all the assets
            # we're interested in
            asset_events = context.latest_materialization_records_by_key(
                self.monitored_asset_keys
            )

            for monitored_asset_key, execution_value in asset_events.items():
                monitored_asset_name = "/".join(monitored_asset_key.path)

                # Add the monitored asset to the list of dynamic partitions of
                # the publishing asset, if it's not already there.
                if monitored_asset_name not in context.instance.get_dynamic_partitions(
                    self.partition_definition_name
                ):
                    context.instance.add_dynamic_partitions(
                        partitions_def_name=self.partition_definition_name,
                        partition_keys=[monitored_asset_name],
                    )

                if execution_value is not None:
                    # Trigger a run request for the publishing asset, if it has
                    # been materialised.
                    # execution_value.run_id contains the run ID of the last time
                    # the monitored asset was materialised. By setting the run_key
                    # to this, we ensure we don't run the publishing sensor
                    # multiple times for the same asset materialisation.
                    yield RunRequest(
                        run_key=f"{monitored_asset_name}_{execution_value.run_id}",
                        partition_key="/".join(monitored_asset_key.path),
                    )
                    context.advance_cursor({monitored_asset_key: execution_value})

        return inner_sensor
