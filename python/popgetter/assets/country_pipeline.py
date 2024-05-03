from __future__ import annotations

from dagster import AssetSelection
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from popgetter.metadata import CountryMetadata


class CountryPipelineDescription:
    """

    Attributes:
      - country: CountryMetadata
      - production_job: None | UnresolvedAssetJobDefinition
            Used to define the Job used production runs of this pipeline. This Job should target only those assets and partitions
            needed for published data.

      - integration_test_jobs: list[UnresolvedAssetJobDefinition]
            Used to define one or more Jobs, for use in integration tests of this pipeline. The Jobs should collectively assert that
            that the implementation of the pipeline correctly understands and uses the external API/datasource, where the relevant
            country data is downloaded from.

        Triggers for downstream pipelines. The following AssetSelection objects are used by downstream AssetSensors to detect when part of this pipeline are complete.
        They do not need to be filtered to only the relevant partitions, as the AssetSensors can filter using the objects type


      - output_metadata: None | AssetSelection
      - output_table_assets: None | AssetSelection
      - output_geometry_assets: None | AssetSelection

    Automatically calculated attributes:

      - asset_prefix:
            Calculated automatically, to be used as the default prefix for all assets in this pipeline. Typically this will be the
            ISO3 country code, in lower case.

      - asset_group:
            Calculated automatically, to be used as the default groupname for all assets in this pipeline. Typically this will be the
            ISO3 country code, in lower case.

    """

    country: CountryMetadata
    production_job: None | UnresolvedAssetJobDefinition
    integration_test_jobs: list[UnresolvedAssetJobDefinition]

    output_metadata: None | AssetSelection

    output_table_assets: None | AssetSelection
    output_geometry_assets: None | AssetSelection
    # In practise the `asset_prefix` and the `asset_group` are likely to have the same
    # value, but we keep them separate for in case we what to revise this in future.
    asset_prefix: str
    asset_group: str

    def __init__(
        self,
        country: CountryMetadata,
        production_job: None | UnresolvedAssetJobDefinition,
        integration_test_jobs: list[UnresolvedAssetJobDefinition],
        output_metadata: None | AssetSelection,
        output_table_assets: None | AssetSelection,
        output_geometry_assets: None | AssetSelection,
    ):
        self.country = country
        self.production_job = production_job
        self.integration_test_jobs = integration_test_jobs
        self.output_metadata = output_metadata
        self.output_table_assets = output_table_assets
        self.output_geometry_assets = output_geometry_assets

        self.asset_prefix = self.country.iso3.lower()
        self.asset_group = self.asset_prefix
