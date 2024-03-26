from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from dagster import (
    AssetKey,
    RunRequest,
    SkipReason,
    asset,
    build_asset_context,
    build_multi_asset_sensor_context,
    instance_for_test,
    materialize_to_memory,
)
from icecream import ic

from popgetter import defs
from popgetter.cloud_outputs import (
    cartography_in_cloud_formats,
    country_outputs_sensor,
    generate_pmtiles,
)
from popgetter.utils import StagingDirResource

# TODO, Move this to a fixture to somewhere more universal
from .test_be import demo_sectors  # noqa: F401


def test_my_asset_sensor():
    @asset
    def irrelevant_asset():
        return 1

    @asset
    def monitored_asset():
        return 2

    # instance = DagsterInstance.ephemeral()
    with instance_for_test() as instance:
        ctx = build_multi_asset_sensor_context(
            monitored_assets=[AssetKey("monitored_asset")],
            instance=instance,
            definitions=defs,
        )

        # Nothing is materialized yet
        result = list(country_outputs_sensor(ctx))
        assert isinstance(result[0], SkipReason)

        # Only the irrelevant asset is materialized
        materialize_to_memory([irrelevant_asset], instance=instance)
        result = list(country_outputs_sensor(ctx))
        assert isinstance(result[0], SkipReason)

        # Only the monitored asset is materialized
        materialize_to_memory([monitored_asset], instance=instance)
        result = list(country_outputs_sensor(ctx))
        assert isinstance(result[0], RunRequest)

        # Both assets are materialized
        materialize_to_memory([monitored_asset, irrelevant_asset], instance=instance)
        result = list(country_outputs_sensor(ctx))
        ic(result[0].run_key)
        ic(result[0].run_config)
        assert isinstance(result[0], RunRequest)


# TODO: The no QA comment below is pending moving the fixture to a more
# universal location
def test_cartography_in_cloud_formats(tmp_path, demo_sectors):  # noqa: F811
    # This test is outputs each of the cartography outputs to individual files
    # in the staging directory. The test then checks that the files exist and
    # that they were created after the test started.
    time_at_start = datetime.now()

    staging_res = StagingDirResource(staging_dir=str(tmp_path))
    resources_for_test = {
        "staging_res": staging_res,
        "unit_test_key": "test_cartography_in_cloud_formats",
    }

    with (
        instance_for_test() as instance,
        build_asset_context(
            resources=resources_for_test,
            instance=instance,
        ) as context,
    ):
        # Collect the results
        results = cartography_in_cloud_formats(
            context, demo_sectors, source_asset_key="demo_sectors"
        )
        output_paths = [r.value for r in list(results)]

        # Check that the output paths exist and were created after the test started
        for output_path in output_paths:
            assert output_path.exists()
            assert output_path.stat().st_mtime > time_at_start.timestamp()


@pytest.mark.skip(reason="Test not implemented yet")
def test_generate_pmtiles():
    input_geojson_path = str(
        Path(__file__).parent / "demo_data" / "be_demo_sector.geojson"
    )

    with build_asset_context() as context:
        generate_pmtiles(context, input_geojson_path)

    # Assert something about the logs exists
    # Assert some output files exist
    pytest.fail("Test not implemented")
