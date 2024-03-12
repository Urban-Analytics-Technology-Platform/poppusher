from __future__ import annotations

from pathlib import Path

from dagster import build_op_context

from popgetter.utils import get_staging_dir


# Test StagingDirectory
def test_staging_directory(tmp_path):
    # Case A
    # Test that if stage_dir is found, then it is used
    context = build_op_context(
        resources={"staging_dir": str(tmp_path / "mock_staging_dir")},
        partition_key="a/b",
    )

    with get_staging_dir(context) as staging_dir_str:
        staging_dir = Path(staging_dir_str)
        assert staging_dir.exists()

        assert staging_dir == tmp_path / "mock_staging_dir" / "a" / "b"
        assert tmp_path in staging_dir.parents

    # Case B
    # Test that if stage_dir not found, then an unrelated temporary directory is used
    context = build_op_context(
        resources={"staging_dir": None},
        partition_key="a/b",
    )

    with get_staging_dir(context) as staging_dir_str:
        staging_dir = Path(staging_dir_str)
        assert staging_dir.exists()

        assert "mock_staging_dir" not in staging_dir_str
        assert staging_dir != tmp_path
        assert tmp_path not in staging_dir.parents
