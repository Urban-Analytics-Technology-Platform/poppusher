from __future__ import annotations

import pytest

# generate_pmtiles,
# from popgetter.cloud_outputs import (
#    cartography_in_cloud_formats,
#    country_outputs_sensor,
# )
# generate_pmtiles,
# TODO, Move this to a fixture to somewhere more universal
from .test_bel import demo_sectors  # noqa: F401

# Commented out test as part of #92 as functions no longer importable
# @pytest.mark.skip(
#     reason="This tests old code that is no longer imported since cloud outputs were reworked in #92"
# )
# def test_country_outputs_sensor():
#     """ ""
#     This test checks that the country_outputs_sensor function returns a RunRequest
#     with the correct AssetKey and PartitionKey.

#     Three upstream assets are mocked, which different qualities.
#     The context object is mocked and this also defines which assets are monitored. Hence the test does not attempt to
#     check that the sensor is correctly monitoring the required production assets.

#     Hence this test only checks that the correct config parameters are returned via the RunRequest object(s).
#     """

#     @asset
#     def irrelevant_asset():
#         return 1

#     @asset
#     def monitored_asset():
#         return 2

#     @asset(
#         partitions_def=StaticPartitionsDefinition(["A", "B", "C"]),
#     )
#     def partitioned_monitored_asset(context):
#         return ic(context.partition_key)

#     # instance = DagsterInstance.ephemeral()
#     with instance_for_test() as instance:
#         ctx = build_multi_asset_sensor_context(
#             monitored_assets=[
#                 AssetKey("monitored_asset"),
#                 AssetKey("partitioned_monitored_asset"),
#             ],
#             instance=instance,
#             definitions=defs,
#         )

#         # Nothing is materialized yet
#         result = list(country_outputs_sensor(ctx))
#         assert len(result) == 1
#         assert isinstance(result[0], SkipReason)

#         # Only the irrelevant asset is materialized
#         materialize_to_memory([irrelevant_asset], instance=instance)
#         result = list(country_outputs_sensor(ctx))
#         assert len(result) == 1
#         assert isinstance(result[0], SkipReason)

#         # Only the monitored asset is materialized
#         materialize_to_memory([monitored_asset], instance=instance)
#         result = list(country_outputs_sensor(ctx))
#         assert len(result) == 1
#         assert isinstance(result[0], RunRequest)

#         # Both non-partitioned assets are materialized
#         materialize_to_memory([monitored_asset, irrelevant_asset], instance=instance)
#         result = list(country_outputs_sensor(ctx))
#         assert len(result) == 1
#         assert isinstance(result[0], RunRequest)
#         assert result[0].partition_key == "monitored-asset"
#         assert (
#             result[0].run_config["ops"]["upstream_df"]["config"]["asset_to_load"]
#             == "monitored_asset"
#         )
#         assert (
#             result[0].run_config["ops"]["upstream_df"]["config"]["partition_to_load"]
#             is None
#         )

#         # All three assets are materialized
#         materialize_to_memory(
#             [monitored_asset, irrelevant_asset, partitioned_monitored_asset],
#             instance=instance,
#             partition_key="A",
#         )
#         result = list(country_outputs_sensor(ctx))
#         assert len(result) == 2

#         # The order of the results does not need to be guaranteed
#         # These two functions are used to check the results below
#         def assert_for_non_partitioned_assets(r):
#             assert isinstance(r, RunRequest)
#             assert r.partition_key == "monitored-asset"
#             assert (
#                 r.run_config["ops"]["upstream_df"]["config"]["asset_to_load"]
#                 == "monitored_asset"
#             )
#             assert (
#                 r.run_config["ops"]["upstream_df"]["config"]["partition_to_load"]
#                 is None
#             )

#         def assert_for_partitioned_assets(r):
#             assert r.partition_key == "partitioned-monitored-asset-a"
#             assert (
#                 r.run_config["ops"]["upstream_df"]["config"]["asset_to_load"]
#                 == "partitioned_monitored_asset"
#             )
#             assert (
#                 r.run_config["ops"]["upstream_df"]["config"]["partition_to_load"] == "A"
#             )

#         # Now check that the results include one partitioned and one non-partitioned asset
#         try:
#             assert_for_non_partitioned_assets(result[0])
#             assert_for_partitioned_assets(result[1])
#         except AssertionError:
#             assert_for_non_partitioned_assets(result[1])
#             assert_for_partitioned_assets(result[0])

# Commented out test as part of #92 as functions no longer importable
# @pytest.mark.skip(
#     reason="This tests old code that is no longer imported since cloud outputs were reworked in #92"
# )
# # TODO: The no QA comment below is pending moving the fixture to a more
# # universal location
# def test_cartography_in_cloud_formats(tmp_path, demo_sectors):
#     # This test is outputs each of the cartography outputs to individual files
#     # in the staging directory. The test then checks that the files exist and
#     # that they were created after the test started.
#     time_at_start = datetime.now()

#     staging_res = StagingDirResource(staging_dir=str(tmp_path))
#     resources_for_test = {
#         "staging_res": staging_res,
#         "unit_test_key": "test_cartography_in_cloud_formats",
#     }

#     with (
#         instance_for_test() as instance,
#         build_asset_context(
#             resources=resources_for_test,
#             instance=instance,
#             partition_key="historic-european-region",
#         ) as context,
#     ):
#         # Collect the results
#         # Results should be a generator which produces three Output objects
#         results = cartography_in_cloud_formats(context, demo_sectors)

#         output_paths = [r.value for r in list(results)]
#         # There should be 3 outputs
#         assert len(output_paths) == 3

#         # Check that the output paths exist and were created after the test started
#         for output_path in output_paths:
#             assert output_path.exists()
#             assert output_path.stat().st_mtime > time_at_start.timestamp()


@pytest.mark.skip(reason="Test not implemented yet")
def test_generate_pmtiles():
    # input_geojson_path = str(
    #     Path(__file__).parent / "demo_data" / "be_demo_sector.geojson"
    # )

    # with build_asset_context(
    #     partition_key="historic-european-region") as context:
    #     generate_pmtiles(context, input_geojson_path)

    # Assert something about the logs exists
    # Assert some output files exist

    pytest.fail("Test not implemented")
