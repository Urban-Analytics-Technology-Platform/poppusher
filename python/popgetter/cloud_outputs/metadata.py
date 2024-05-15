from dagster import (
    multi_asset_sensor,
    asset,
    AssetSelection,
    AssetExecutionContext,
    AssetKey,
    StaticPartitionsDefinition,
    DefaultSensorStatus,
    RunRequest,
    Output,
    define_asset_job,
    load_assets_from_current_module,
)
from icecream import ic
from functools import reduce

cloud_assets_metadata = load_assets_from_current_module(
    group_name="cloud_assets_metadata"
)

# TODO: is there a better way to do this than to manually list them here?
toplevel_metadata_asset_names = [
    "be/country_metadata",
    "be/data_publisher",
    "be/source_data_release",
]
toplevel_metadata_partition = StaticPartitionsDefinition(toplevel_metadata_asset_names)
toplevel_metadata_assets_to_monitor = reduce(
    lambda x, y: x | y,
    [AssetSelection.keys(k) for k in toplevel_metadata_asset_names],
)


@asset(
    partitions_def=toplevel_metadata_partition,
    io_manager_key="publishing_io_manager",
)
def publish_toplevel_metadata(context: AssetExecutionContext):
    # Get the output of the asset
    from popgetter import defs as popgetter_defs

    ic(context.partition_key)

    # load_asset_value expects a list of strings
    output = popgetter_defs.load_asset_value(context.partition_key.split("/"))
    ic(output)

    return Output(output)


toplevel_metadata_job = define_asset_job(
    name="toplevel_metadata_job",
    selection="publish_toplevel_metadata",
    partitions_def=toplevel_metadata_partition,
)


@multi_asset_sensor(
    monitored_assets=toplevel_metadata_assets_to_monitor,
    job=toplevel_metadata_job,
    minimum_interval_seconds=10,
    default_status=DefaultSensorStatus.RUNNING,
)
def metadata_sensor(context):
    asset_events = context.latest_materialization_records_by_key()
    for asset_key, execution_value in asset_events.items():
        yield RunRequest(
            run_key=None,
            partition_key="/".join(asset_key.path),
        )
        context.advance_cursor({asset_key: execution_value})
