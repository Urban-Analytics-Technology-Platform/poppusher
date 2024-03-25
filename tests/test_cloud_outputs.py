from __future__ import annotations

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
from popgetter.cloud_outputs import cartography_in_cloud_formats, country_outputs_sensor


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


def test_cartography_in_cloud_formats(demo_sectors):
    with build_asset_context() as context:
        result = cartography_in_cloud_formats(context, demo_sectors)
        ic(result)

    pytest.fail("Test not complete yet")
