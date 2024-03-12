from __future__ import annotations

import base64
import datetime
import io
import subprocess
import xml.etree.ElementTree as ET
import zipfile
from io import BytesIO
from pathlib import Path

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
