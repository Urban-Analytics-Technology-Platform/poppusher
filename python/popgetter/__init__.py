from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from .utils import StagingDirResource

__version__ = "0.1.0"

__all__ = ["__version__"]


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

from . import assets, cloud_outputs

all_assets: Sequence[AssetsDefinition | SourceAsset | CacheableAssetsDefinition] = [
    *load_assets_from_package_module(assets.us, group_name="us"),
    *load_assets_from_package_module(assets.be, group_name="be"),
    *load_assets_from_package_module(assets.uk, group_name="uk"),
    *load_assets_from_modules([cloud_outputs], group_name="cloud_assets"),
]

# job_be: UnresolvedAssetJobDefinition = define_asset_job(
#     name="job_be",
#     selection=AssetSelection.groups("be"),
#     description="Downloads Belgian data.",
#     partitions_def=assets.be.census_tables.dataset_node_partition,
# )

job_be_production = assets.be.belgium.pipeline.production_job
# assets.be.belgium.

job_be_integration = assets.be.belgium.pipeline.integration_test_jobs
# country_pipeline

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

all_jobs = [job_us, job_uk, *job_be_integration]

if job_be_production is not None:
    all_jobs.append(job_be_production)


defs: Definitions = Definitions(
    assets=all_assets,
    schedules=[],
    sensors=[cloud_outputs.country_outputs_sensor],
    resources={
        "pipes_subprocess_client": PipesSubprocessClient(),
        "staging_res": StagingDirResource(
            staging_dir=str(Path(__file__).parent.joinpath("staging_dir").resolve())
        ),
    },
    jobs=all_jobs,
)
