from dagster import (
    load_assets_from_package_module,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
    PipesSubprocessClient
)

from popgetter import assets
import os

all_assets = [
    *load_assets_from_package_module(assets.us, group_name="us"),
    *load_assets_from_package_module(assets.be, group_name="be"),
    *load_assets_from_package_module(assets.uk, group_name="uk"),
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

job_uk = define_asset_job(
    name="job_uk",
    selection=AssetSelection.groups("uk"),
    description="Downloads UK data.",
)

defs = Definitions(
    assets=all_assets,
    schedules=[],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
    jobs=[job_be, job_us, job_uk],
)
