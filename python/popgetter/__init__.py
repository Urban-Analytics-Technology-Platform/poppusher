from __future__ import annotations

import os
import warnings
from collections.abc import Sequence
from pathlib import Path

from dagster import ExperimentalWarning

# Has to be placed before imports as otherwise importing other modules will
# trigger ExperimentalWarnings themselves...
if "IGNORE_EXPERIMENTAL_WARNINGS" in os.environ:
    warnings.filterwarnings("ignore", category=ExperimentalWarning)

from popgetter.io_managers.azure import (
    AzureGeneralIOManager,
    AzureGeoIOManager,
    AzureMetadataIOManager,
    AzureMetricsIOManager,
    AzureMetricsMetadataIOManager,
    AzureMetricsSingleIOManager,
)
from popgetter.io_managers.local import (
    LocalGeoIOManager,
    LocalMetadataIOManager,
    LocalMetricsIOManager,
    LocalMetricsMetadataIOManager,
    LocalMetricsSingleIOManager,
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
    load_assets_from_modules,
    load_assets_from_package_module,
)
from dagster._core.definitions.cacheable_assets import (
    CacheableAssetsDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from popgetter import azure_test, cloud_outputs
from popgetter.assets import countries

PROD = os.getenv("ENV") == "prod"

all_assets: Sequence[AssetsDefinition | SourceAsset | CacheableAssetsDefinition] = [
    *[
        asset
        for (module, name) in countries
        for asset in load_assets_from_package_module(module, group_name=name)
    ],
    *load_assets_from_package_module(cloud_outputs, group_name="cloud_outputs"),
    *(load_assets_from_modules([azure_test], group_name="azure_test") if PROD else []),
]

jobs: list[UnresolvedAssetJobDefinition] = [
    define_asset_job(
        name=f"job_{country_name}",
        selection=AssetSelection.groups(country_name),
        description=f"Downloads data for country '{country_name}'.",
    )
    for (_, country_name) in countries
]


def resources_by_env():
    return (
        {
            "metadata_io_manager": AzureMetadataIOManager(),
            "geometry_io_manager": AzureGeoIOManager(),
            "metrics_io_manager": AzureMetricsIOManager(),
            "azure_general_io_manager": AzureGeneralIOManager(".bin"),
            "metrics_single_io_manager": AzureMetricsSingleIOManager(),
            "metrics_metadata_io_manager": AzureMetricsMetadataIOManager(),
        }
        if PROD
        else {
            "metadata_io_manager": LocalMetadataIOManager(),
            "geometry_io_manager": LocalGeoIOManager(),
            "metrics_io_manager": LocalMetricsIOManager(),
            "metrics_single_io_manager": LocalMetricsSingleIOManager(),
            "metrics_metadata_io_manager": LocalMetricsMetadataIOManager(),
        }
    )


resources = {
    "pipes_subprocess_client": PipesSubprocessClient(),
    "staging_res": StagingDirResource(
        staging_dir=str(Path(__file__).parent.joinpath("staging_dir").resolve())
    ),
} | resources_by_env()


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
