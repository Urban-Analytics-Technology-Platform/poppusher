from __future__ import annotations

import base64
import datetime
import io
import subprocess
import xml.etree.ElementTree as ET
import zipfile
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import fsspec
import requests
from dagster import (
    ConfigurableResource,
    EnvVar,
    get_dagster_logger,
)

DOWNLOAD_ROOT = Path(__file__).parent.absolute() / "data"
CACHE_ROOT = Path(__file__).parent.absolute() / "cache"


class SourceDataAssumptionsOutdated(ValueError):
    """
    Raised when a DAG detected a situation that implies there has been a change to
    the assumed properties of the upstream data.
    Typically this error implies that the DAG will need to be retested and updated.
    """


class StagingDirResource(ConfigurableResource):
    staging_dir: str | None


def markdown_from_plot(plot) -> str:
    plot.tight_layout()

    # Convert the image to a saveable format
    buffer = BytesIO()
    plot.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    return f"![img](data:image/png;base64,{image_data.decode()})"


def _last_update(file_path):
    """
    Returns the date and time of the last update to the file at `file_path`.
    """
    _path = Path(file_path)
    if not _path.exists():
        return None
    last_update = _path.stat().st_mtime
    return datetime.datetime.fromtimestamp(last_update)


def get_staging_dir(
    asset_context, staging_res: StagingDirResource
) -> StagingDirectory | TemporaryDirectory:
    """
    This creates a "staging directory" resource. This is a convenience feature for developers,
    allowing large downloads to be cached locally and not repeatedly downloaded during development.

    - The root of the staging directory is specified by the `staging_dir` resource.
    - If staging_dir is `None` then a [TemporaryDirectory](https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory)
    will be returned.
    - If staging_dir is not specified, a ValueError is raised.

    The staging directory must be separate from, and must _not_ contained within, the Dagster-managed
    asset storage (as specified by DAGSTER_HOME). A ValueError will be raised if this is not the case.

    Internally, it is organised using asset-prefix / asset-name / [partition-name] to ensure that
    each asset has an (initially) empty directory with which to work.Typically the directory returned
    will have the path structure:

    ```
    staging_dir_root / asset_prefix / asset_name / [partition_name]
    ```

    If called from a multi-asset function, the directory will have the path structure:

    ```
    staging_dir_root / multi_asset_function_name
    ```

    There is no cache validation/invalidation logic (nor is any planned). The developer is responsible
    for clearing/deleting its contents when necessary.

    The "staging directory" should typically be used where TemporaryDirectory might be used.
    The "staging directory" is not intended for use in production deployments.
    """
    if staging_res.staging_dir is None:
        return TemporaryDirectory()

    return StagingDirectory(asset_context, staging_res.staging_dir)


class StagingDirectory:
    def __init__(self, asset_context, staging_root: str):
        self.staging_dir: Path = self._get_staging_dir(asset_context, staging_root)
        self._check_against_dagster_home()

    def _check_against_dagster_home(self):
        # Check that the staging directory is not the same as, or contained within DAGSTER_HOME
        dagster_home_str = EnvVar("DAGSTER_HOME").get_value(None)
        if dagster_home_str:
            dagster_home = Path(dagster_home_str)
            dagster_home = dagster_home.resolve()

            if dagster_home == self.staging_dir:
                err_msg = "DAGSTER_HOME and staging_dir are the same"
                raise ValueError(err_msg)

            if dagster_home in self.staging_dir.parents:
                err_msg = "DAGSTER_HOME is a parent of staging_dir"
                raise ValueError(err_msg)

    # TODO: should this be a class method or instance method?
    def _get_staging_dir(self, asset_context, staging_root: str) -> Path:
        root = Path(staging_root)
        root = root.resolve()

        if asset_context.op_config and "unit_test_key" in asset_context.op_config:
            # TODO: This is unsatisfactory, as it is necessary to have code that handle the specific
            # conditions of running in a test environment. The `context` object in a test
            # context is not a perfect replica of the `context` object. There are many
            # attributes that are not present in the test context.
            #
            # Any attempt to access the context properties, including checking for them
            # using `hasattr` throws an `dagster._core.errors.DagsterInvalidPropertyError`
            # error. This is not caught by either `DagsterError` or `DagsterUserCodeExecutionError`.
            get_dagster_logger().info(
                "This is unsatisfactory, as it is necessary to have code that handle the specific"
                " conditions of running in a test environment. The `context` object in a test"
                " context is not a perfect replica of the `context` object. There are many "
                " attributes that are not present in the test context. "
                " Any attempt to access the context properties, including checking for them"
                " using `hasattr` throws an `dagster._core.errors.DagsterInvalidPropertyError`"
                " error. This is not caught by either `DagsterError` or `DagsterUserCodeExecutionError`."
            )

            staging_dir = root / asset_context.op_config["unit_test_key"]
        elif len(asset_context.selected_asset_keys) == 1:
            asset_key = asset_context.asset_key_for_output()
            staging_dir = root.joinpath(*asset_key.path)
        else:
            # Assuming that this is being called from a function that returns
            # a multi_asset, hence there is no single asset key to use
            # `asset_context.op_handle.name` is the name of the multi_asset function
            multi_asset_func_name = asset_context.op_handle.name
            staging_dir = root.joinpath(multi_asset_func_name)

        # Add in the partition key if it exists
        if asset_context.has_partition_key:
            partition_key = asset_context.partition_key
            staging_dir = staging_dir / partition_key

        return staging_dir

    def __enter__(self) -> str:
        # Ensure staging_dir exists
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        get_dagster_logger().info(f"Using staging directory: {self.staging_dir}")
        return str(self.staging_dir)

    def __exit__(self, exc_type, exc_value, traceback):
        get_dagster_logger().info(f"Exiting staging directory: {self.staging_dir}")


def extract_main_file_from_zip(temp_zip_file, temp_dir, expected_extension) -> str:
    """
    For cases where the downloaded file (a) is a zip file, (b) contains multiple files,
    and (c) we only want one of those files, (e.g the other files are metadata or documentation
    files), this function extracts the main file from a zip file.

    Parameters:
    -----------
    temp_file : str
        The path to the downloaded file.
    temp_dir : str
        The path to the directory where the file will be extracted.
    expected_extension : str
        The file extension of the file we want to extract. It is assumed that there will only be
        one file with this extension in the zip file.
    """
    source_archive_file_path = expected_extension
    # We have a zip file, so extract the file we want from that
    with zipfile.ZipFile(temp_zip_file, "r") as z:
        # if there is only one file in the zip, we can just extract it
        zip_contents = z.namelist()

        # If there is only one file in the zip, we can just extract it, ignoring the specified file path
        if len(zip_contents) == 1:
            source_archive_file_path = zip_contents[0]
        elif sum([f.endswith(expected_extension) for f in zip_contents]) == 1:
            # If there are multiple files in the zip, but only one has the correct file extension, we can just extract it, ignoring the specified file path
            for f in zip_contents:
                if f.endswith(expected_extension):
                    source_archive_file_path = f

        # Extract the file we want, assuming that we've found the identified the correct file
        if source_archive_file_path in zip_contents:
            z.extract(source_archive_file_path, path=temp_dir)
            return str(temp_dir / source_archive_file_path)

        # If we get here, we have not found the file we want
        err_msg = (
            f"Could not find {source_archive_file_path} in the zip file {temp_zip_file}"
        )
        raise ValueError(err_msg)


def download_from_wfs(wfs_url: str, output_file: str) -> None:
    """
    Downloads data from a WFS (`wfs_url`) and saves it to a file (`output_file`).

    The `ogr2ogr` command line tool is used to workaround the feature count limit that can be imposed by the server. (See https://gdal.org/drivers/vector/wfs.html#request-paging and https://gis.stackexchange.com/questions/422609/downloading-lots-of-data-from-wfs for details).

    Parameters
    ----------
    wfs_url : str
        The URL of the WFS.
    output_file : str
        The name of the output file.
    """
    template = r"""
        <OGRWFSDataSource>
            <URL>CHANGE_ME</URL>
            <PagingAllowed>ON</PagingAllowed>
            <PageSize>1000</PageSize>
        </OGRWFSDataSource>
    """

    # Writing OGR Virtual Format file
    root = ET.fromstring(
        template
    )  # This will return None is the XML template is invalid, or and Element if it is valid.
    root.find("URL").text = wfs_url  # type: ignore (In this case we can be confident that the template is valid)

    with Path(f"{output_file}.xml").open(mode="w") as f:
        f.write(ET.tostring(root).decode())

    # Running ogr2ogr
    # *Assuming* that ogr2ogr returns zero on success (have not confirmed this independently)
    subprocess.run(
        ["ogr2ogr", "-f", "GeoJSON", f"{output_file}.geojson", f"{output_file}.xml"],
        check=True,
    )

    # Done


def get_path_to_cache(
    url: str, cache_path: Path, mode: str = "b"
) -> fsspec.core.OpenFiles:
    """
    Returns the path(s) to the local cached files for a given URL.
    Downloads the file if it is not already cached.
    """
    cache_storage = str(CACHE_ROOT / cache_path)

    # If a cache path is provided, enforce the use of simplecache
    if cache_path and not url.startswith("simplecache::"):
        url = f"simplecache::{url}"

    if ("w" not in mode) and ("r" not in mode):
        mode = f"r{mode}"

    return fsspec.open(
        url,
        mode,
        simplecache={
            "cache_storage": cache_storage,
            "check_files": False,
        },
    )


def download_zipped_files(zipfile_url: str, output_dir: str) -> None:
    """
    Downloads a zipped file from a URL and extracts it to a folder.

    Raises a ValueError if the output folder is not empty.
    """
    output_dpath = Path(output_dir).absolute()
    output_dpath.mkdir(parents=True, exist_ok=True)

    if any(output_dpath.iterdir()):
        err_msg = f"Directory {output_dpath} is not empty. Skipping download."
        raise ValueError(err_msg)

    r = requests.get(zipfile_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(output_dpath)


if __name__ == "__main__":
    pass
    # This is for testing only
    # oa_wfs_url: str = "https://dservices1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_/WFSServer?service=wfs",
    # layer_name: str = "Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW"

    # serviceItemId taken from:
    # https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_2022/FeatureServer/0?f=pjson
    # serviceItemId: str = "6c6743e1e4b444f6afcab9d9588f5d8f"

    # download_from_wfs(oa_wfs_url, layer_name)
    # download_from_arcgis_online(serviceItemId, "data/oa_from_agol2.geojson")
