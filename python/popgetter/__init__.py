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
    local_file_manager,
)
from dagster._core.definitions.cacheable_assets import (
    CacheableAssetsDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster_azure.adls2 import adls2_file_manager

from popgetter import assets

all_assets: Sequence[AssetsDefinition | SourceAsset | CacheableAssetsDefinition] = [
    *load_assets_from_package_module(assets.us, group_name="us"),
    *load_assets_from_package_module(assets.be, group_name="be"),
    *load_assets_from_package_module(assets.uk, group_name="uk"),
]

job_be: UnresolvedAssetJobDefinition = define_asset_job(
    name="job_be",
    selection=AssetSelection.groups("be"),
    description="Downloads Belgian data.",
    partitions_def=assets.be.census_tables.dataset_node_partition,
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

resources = {
    "DEV": {"publishing_file_manager": local_file_manager},
    "PRODUCTION": {
        "publishing_file_manager": adls2_file_manager(
            # See https://docs.dagster.io/_apidocs/libraries/dagster-azure#dagster_azure.adls2.adls2_file_manager
            storage_account="tbc",  # The storage account name.
            credential={},  # The credential used to authenticate the connection.
            adls2_file_system="tbc",
            adls2_prefix="tbc",
        )
    },
}


defs: Definitions = Definitions(
    assets=all_assets,
    schedules=[],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
    jobs=[job_be, job_us, job_uk],
)
