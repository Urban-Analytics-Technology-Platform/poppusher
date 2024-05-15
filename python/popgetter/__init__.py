from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from popgetter.io_managers.azure import (
    AzureGeneralIOManager,
    AzureGeoIOManager,
    AzureTopLevelMetadataIOManager,
)
from popgetter.io_managers.local import (
    LocalGeoIOManager,
    LocalTopLevelMetadataIOManager,
)
from popgetter.utils import StagingDirResource

__version__ = "0.1.0"

__all__ = ["__version__"]


import os

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

from popgetter import assets, cloud_outputs

all_assets: Sequence[AssetsDefinition | SourceAsset | CacheableAssetsDefinition] = [
    *load_assets_from_package_module(assets.us, group_name="us"),
    *load_assets_from_package_module(assets.be, group_name="be"),
    *load_assets_from_package_module(assets.uk, group_name="uk"),
    *load_assets_from_package_module(cloud_outputs, group_name="cloud_outputs"),
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

resources_by_env = {
    "prod": {
        "metadata_io_manager": AzureTopLevelMetadataIOManager(),
        "geometry_io_manager": AzureGeoIOManager(),
    },
    "dev": {
        "metadata_io_manager": LocalTopLevelMetadataIOManager(),
        "geometry_io_manager": LocalGeoIOManager(),
    },
}

resources = {
    "pipes_subprocess_client": PipesSubprocessClient(),
    "staging_res": StagingDirResource(
        staging_dir=str(Path(__file__).parent.joinpath("staging_dir").resolve())
    ),
    "azure_general_io_manager": AzureGeneralIOManager(".bin"),
}

resources.update(resources_by_env[os.getenv("ENV", "dev")])

defs: Definitions = Definitions(
    assets=all_assets,
    schedules=[],
    sensors=[
        cloud_outputs.metadata_sensor,
        cloud_outputs.geometry_sensor,
    ],
    resources=resources,
    jobs=[job_be, job_us, job_uk],
)
