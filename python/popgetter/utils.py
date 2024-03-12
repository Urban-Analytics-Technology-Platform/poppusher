from __future__ import annotations

import base64
import datetime
import io
import subprocess
import xml.etree.ElementTree as ET
import zipfile
from io import BytesIO
from pathlib import Path
from dagster import op, EnvVar
from tempfile import TemporaryDirectory
from icecream import ic

import fsspec
import requests

DOWNLOAD_ROOT = Path(__file__).parent.absolute() / "data"
CACHE_ROOT = Path(__file__).parent.absolute() / "cache"


class SourceDataAssumptionsOutdated(ValueError):
    """
    Raised when a DAG detected a situation that implies there has been a change to
    the assumed properties of the upstream data.
    Typically this error implies that the DAG will need to be retested and updated.
    """


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



@op(required_resource_keys={"staging_dir"})
def get_staging_dir(context):
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

    There is no cache validation/invalidation logic (nor is any planned). The developer is responsible
    for clearing/deleting its contents when necessary.

    The "staging directory" should typically be used where TemporaryDirectory might be used.
    The "staging directory" is not intended for use in production deployments.
    """
    if context.resources.staging_dir is None:
        return TemporaryDirectory()
    
    return StagingDirectory(context)


class StagingDirectory:

    def __init__(self, context):
        self.staging_dir = self._get_staging_dir(context)
        self._check_against_dagster_home()


    def _check_against_dagster_home(self):
        # Check that the staging directory is not the same as, or contained within DAGSTER_HOME
        dagster_home_str = EnvVar("DAGSTER_HOME").get_value(None)
        if dagster_home_str:
            ic(dagster_home_str)

            dagster_home = Path(dagster_home_str)
            dagster_home = dagster_home.resolve()

            if dagster_home == self.staging_dir:
                err_msg = "DAGSTER_HOME and staging_dir are the same"
                raise ValueError(err_msg)

            if dagster_home in self.staging_dir.parents:
                err_msg = "DAGSTER_HOME is a parent of staging_dir"
                raise ValueError(err_msg)


    # TODO: should this be a class method or instance method?
    def _get_staging_dir(self, context) -> Path:
        try:
            root_str = context.resources.staging_dir
        except AttributeError as attrib_error:
            err_msg = "No staging_dir resource found"
            ic(err_msg)
            raise ValueError(err_msg) from attrib_error

        root = Path(root_str)
        root = root.resolve()
    
        # Now try and create subdirectory based on the asset and partition keys
        asset_key = context.asset_key_for_output()
        ic(asset_key)
        ic(asset_key.path)

        staging_dir = root.joinpath(*asset_key.path)

        if context.has_partition_key:
            partition_key = context.partition_key
            staging_dir = staging_dir / partition_key

        ic(staging_dir)
        return staging_dir
    
    def __enter__(self):
        # Ensure staging_dir exists
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        ic(f"Using staging directory: {self.staging_dir}")
        return self

    def __exit__(self):
        ic(f"Existing staging directory: {self.staging_dir}")
        pass


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
