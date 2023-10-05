from dagster import (
    load_assets_from_package_module,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
)

from popgetter import assets
import os

defs = Definitions(
    assets=load_assets_from_package_module(assets),
    schedules=[],
    resources={},
)
