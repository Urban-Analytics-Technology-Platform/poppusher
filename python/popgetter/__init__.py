from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

# from python.popgetter.utils import StagingDirResource
from popgetter.utils import StagingDirResource

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
from popgetter.assets.scotland.census_tables import (
    dataset_node_partition as dataset_partition_scotland,
)

all_assets: Sequence[AssetsDefinition | SourceAsset | CacheableAssetsDefinition] = [
    *load_assets_from_package_module(assets.us, group_name="us"),
    *load_assets_from_package_module(assets.be, group_name="be"),
    *load_assets_from_package_module(assets.uk, group_name="uk"),
    *load_assets_from_package_module(
        assets.scotland, group_name="scotland", key_prefix="uk-scotland"
    ),
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

job_scotland: UnresolvedAssetJobDefinition = define_asset_job(
    name="job_scotland",
    selection=AssetSelection.groups("scotland"),
    description="Downloads Scotland data.",
    partitions_def=dataset_partition_scotland,
    # https://docs.dagster.io/guides/limiting-concurrency-in-data-pipelines#asset-based-jobs
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 20,  # limits concurrent assets
                },
            }
        }
    },
)


defs: Definitions = Definitions(
    assets=all_assets,
    schedules=[],
    # Example with multiple configs including for production:
    # https://docs.dagster.io/guides/dagster/transitioning-data-pipelines-from-development-to-production#production
    resources={
        "pipes_subprocess_client": PipesSubprocessClient(),
        "staging_res": StagingDirResource(
            staging_dir=str(Path(__file__).parent.joinpath("staging_dir").resolve())
        ),
    },
    jobs=[job_be, job_us, job_uk, job_scotland],
)
