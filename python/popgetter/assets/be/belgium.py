from __future__ import annotations

from dagster import (
    AssetSelection,
    define_asset_job,
)

from popgetter.assets.country_pipeline import CountryPipelineDescription
from popgetter.metadata import CountryMetadata

from . import census_derived, census_tables

country: CountryMetadata = CountryMetadata(
    name_short_en="Belgium",
    name_official="Kingdom of Belgium",
    iso3="BEL",
    iso2="BE",
)

_pipeline: CountryPipelineDescription = CountryPipelineDescription(
    country=country,
    production_job=None,
    integration_test_jobs=[],
    output_metadata=None,
    output_table_assets=None,
    output_geometry_assets=None,
)

_pipeline.output_table_assets = AssetSelection.groups(_pipeline.asset_group) & (
    AssetSelection.keys(_pipeline.asset_prefix, "source_table")
    | AssetSelection.keys(_pipeline.asset_prefix, "derived_table")
)

_pipeline.output_metadata = AssetSelection.groups(_pipeline.asset_group) & (
    AssetSelection.keys(_pipeline.asset_prefix, "source_mmd")
    | AssetSelection.keys(_pipeline.asset_prefix, "derived_mmd")
)


_pipeline.production_job = define_asset_job(
    name="job_be",
    selection=AssetSelection.groups(_pipeline.asset_group),
    description="Downloads Belgian data.",
    partitions_def=census_derived.needed_dataset_partition,
)

_pipeline.integration_test_jobs.append(
    define_asset_job(
        name="job_be",
        selection=AssetSelection.groups(_pipeline.asset_group),
        description="Downloads Belgian data.",
        partitions_def=census_tables.dataset_node_partition,
    )
)

pipeline = _pipeline

# load_assets_from_package_module(assets.be, group_name="be"),

# load_assets_from_current_module()


# WORKING_DIR = Path("belgium")
asset_prefix = pipeline.asset_prefix
