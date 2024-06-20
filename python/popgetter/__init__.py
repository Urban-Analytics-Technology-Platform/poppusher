from __future__ import annotations

import os
import warnings
from collections.abc import Sequence
from pathlib import Path

from dagster import ExperimentalWarning

from popgetter.io_managers.azure import (
    AzureGeneralIOManager,
    AzureGeoIOManager,
    AzureMetadataIOManager,
    AzureMetricsIOManager,
)
from popgetter.io_managers.local import (
    LocalGeoIOManager,
    LocalMetadataIOManager,
    LocalMetricsIOManager,
)
from popgetter.utils import StagingDirResource

__version__ = "0.1.0"

__all__ = ["__version__"]


if "IGNORE_EXPERIMENTAL_WARNINGS" in os.environ:
    warnings.filterwarnings("ignore", category=ExperimentalWarning)


import os

from dagster import (
    AssetsDefinition,
    AssetSelection,
    Definitions,
    PipesSubprocessClient,
    SourceAsset,
    define_asset_job,
    load_assets_from_modules,
    load_assets_from_package_module,
)
from dagster._core.definitions.cacheable_assets import (
    CacheableAssetsDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from popgetter import assets, azure_test, cloud_outputs

all_assets: Sequence[AssetsDefinition | SourceAsset | CacheableAssetsDefinition] = [
    *load_assets_from_package_module(assets.us, group_name="us"),
    *load_assets_from_package_module(assets.be, group_name="be"),
    *load_assets_from_package_module(assets.uk, group_name="uk"),
    *load_assets_from_package_module(assets.ni, group_name="ni"),
    *load_assets_from_package_module(cloud_outputs, group_name="cloud_outputs"),
    *(
        load_assets_from_modules([azure_test], group_name="azure_test")
        if os.getenv("ENV") == "prod"
        else []
    ),
]

jobs: list[UnresolvedAssetJobDefinition] = [
    define_asset_job(
        name="job_be",
        selection=AssetSelection.groups("be"),
        description="Downloads Belgian data.",
        partitions_def=assets.be.census_tables.dataset_node_partition,
    ),
    define_asset_job(
        name="job_us",
        selection=AssetSelection.groups("us"),
        description="Downloads USA data.",
    ),
    define_asset_job(
        name="job_uk",
        selection=AssetSelection.groups("uk"),
        description="Downloads UK data.",
    ),
    define_asset_job(
        name="job_ni",
        selection=AssetSelection.groups("ni"),
        description="Downloads Northern Ireland data.",
    ),
    define_asset_job(
        name="job_ew_census",
        selection=load_assets_from_modules([assets.uk.england_wales_census]),
        description="Downloads England and Wales census data.",
    ),
]


def resources_by_env():
    env = os.getenv("ENV", "dev")
    if env == "prod":
        return {
            "metadata_io_manager": AzureMetadataIOManager(),
            "geometry_io_manager": AzureGeoIOManager(),
            "metrics_io_manager": AzureMetricsIOManager(),
            "azure_general_io_manager": AzureGeneralIOManager(".bin"),
        }
    if env == "dev":
        return {
            "metadata_io_manager": LocalMetadataIOManager(),
            "geometry_io_manager": LocalGeoIOManager(),
            "metrics_io_manager": LocalMetricsIOManager(),
        }

    err = f"$ENV should be either 'dev' or 'prod', but received '{env}'"
    raise ValueError(err)


resources = {
    "pipes_subprocess_client": PipesSubprocessClient(),
    "staging_res": StagingDirResource(
        staging_dir=str(Path(__file__).parent.joinpath("staging_dir").resolve())
    ),
}

resources.update(resources_by_env())

defs: Definitions = Definitions(
    assets=all_assets,
    schedules=[],
    sensors=[
        cloud_outputs.metadata_sensor,
        cloud_outputs.geometry_sensor,
        cloud_outputs.metrics_sensor,
    ],
    resources=resources,
    jobs=jobs,
)
