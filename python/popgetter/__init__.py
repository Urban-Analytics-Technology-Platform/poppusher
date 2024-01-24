from __future__ import annotations

from collections.abc import Sequence

__version__ = "0.1.0"

__all__ = ["__version__"]


from dagster import (
    AssetsDefinition,
    AssetSelection,
    Definitions,
    PipesSubprocessClient,
    SourceAsset,
    define_asset_job,
    load_assets_from_package_module,
)
from dagster._core.definitions.cacheable_assets import (
    CacheableAssetsDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from popgetter import assets

all_assets: Sequence[AssetsDefinition | SourceAsset | CacheableAssetsDefinition] = [
    # *load_assets_from_package_module(assets.us, group_name="us"),
    # *load_assets_from_package_module(assets.be, group_name="be"),
    # *load_assets_from_package_module(assets.uk, group_name="uk"),
    *load_assets_from_package_module(
        assets.demo_for_tech_talk, group_name="demo", key_prefix="demo"
    ),
]

job_demo: UnresolvedAssetJobDefinition = define_asset_job(
    name="job_demo",
    selection=AssetSelection.groups("demo"),
    description="Downloads subset of Belgian data for demo purposes.",
)

job_be: UnresolvedAssetJobDefinition = define_asset_job(
    name="job_be",
    selection=AssetSelection.groups("be"),
    description="Downloads Belgian data.",
)

job_us: UnresolvedAssetJobDefinition = define_asset_job(
    name="job_us",
    selection=AssetSelection.groups("us"),
    description="Downloads USA data.",
)

job_uk: UnresolvedAssetJobDefinition = define_asset_job(
    name="job_uk",
    selection=AssetSelection.groups("uk"),
    description="Downloads UK data.",
)

defs: Definitions = Definitions(
    assets=all_assets,
    schedules=[],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
    jobs=[job_be, job_us, job_uk, job_demo],
)
