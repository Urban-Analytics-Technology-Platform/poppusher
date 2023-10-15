from dagster import (
    load_assets_from_package_module,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection
)

from popgetter import assets
import os

all_assets = [
    *load_assets_from_package_module(assets.us, group_name="us"),
    *load_assets_from_package_module(assets.be, group_name="be"),
]

job_be = define_asset_job(
    name="job_be",
    selection=AssetSelection.groups("be"),
    description="Downloads Belgian data.",
)

job_us = define_asset_job(
    name="job_us",
    selection=AssetSelection.groups("us"),
    description="Downloads USA data.",
)

defs = Definitions(
    assets=all_assets,
    schedules=[],
    resources={},
    jobs=[job_be, job_us],
)
