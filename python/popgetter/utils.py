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


def download_from_wfs(wfs_url, output_file):
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
    root = ET.fromstring(template)
    root.find("URL").text = wfs_url

    with Path(f"{output_file}.xml").open(mode="w") as f:
        f.write(ET.tostring(root).decode())

    # Running ogr2ogr
    # *Assuming* that ogr2ogr returns zero on success (have not confirmed this independently)
    subprocess.run(
        ["ogr2ogr", "-f", "GeoJSON", f"{output_file}.geojson", f"{output_file}.xml"],
        check=True,
    )

    # Done


def download_from_arcgis_online(serviceItemId, output_file, force=False):
    """
    Downloads data from ArcGIS Online and saves it to a file (`output_file`). This function can only download data that is available to anonymous users.
    The data will only be downloaded if the output file does not exist, or if the data on ArcGIS Online has been updated since the output file was last updated. Use `force=True` will cause the data to be re-downloaded if it an up-to-date file exists locally.
    """
    try:
        from arcgis.gis import GIS
    except ImportError as import_error:
        err_msg = "Unable to import `arcgis`. Please install the `arcgis` package, using the command `pip install -r requirements-non-foss.txt."
        raise ValueError(err_msg) from import_error

    # Anonymous access to ArcGIS Online
    gis = GIS()

    # Get the `Item`, then, `FeatureLayer` then 'FeatureSet`:
    #  item metadata: {agol_item.metadata}
    agol_item = gis.content.get(serviceItemId)

    agol_layer = agol_item.layers[0]

    # Get the last edit datetime for the layer
    # layer properties: {agol_layer.properties}
    lyr_props = agol_layer.properties
    # Epoch time in milliseconds - convert to datetime
    lyr_last_edit = lyr_props.get("editingInfo", {}).get("lastEditDate", None)
    if lyr_last_edit:
        lyr_last_edit = datetime.datetime.fromtimestamp(lyr_last_edit / 1000)

    # If the output file exists, check the last edit time
    output_last_edit = _last_update(output_file)

    if (
        not force
        and output_last_edit
        and lyr_last_edit
        and output_last_edit > lyr_last_edit
    ):
        # Output file is up-to-date
        return

    # Output file is out-of-date
    agol_feature_set = agol_layer.query()

    # Write to geojson file
    with Path(output_file).open(mode="w") as f:
        f.write(agol_feature_set.to_geojson)

    # Done


def get_path_to_cache(
    url: str,
    cache_path: Path | None,
    mode="b",
) -> fsspec.core.OpenFile:
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


def download_zipped_files(zipfile_url, output_dpath):
    """
    Downloads a zipped file from a URL and extracts it to a folder.

    Raises a ValueError if the output folder is not empty.
    """
    output_dpath = Path(output_dpath).absolute()
    output_dpath.mkdir(parents=True, exist_ok=True)

    if any(output_dpath.iterdir()):
        err_msg = f"Directory {output_dpath} is not empty. Skipping download."
        raise ValueError(err_msg)

    r = requests.get(zipfile_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(output_dpath)


if __name__ == "__main__":
    # This is for testing only
    oa_wfs_url = (
        "https://dservices1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_/WFSServer?service=wfs",
    )
    layer_name = "Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW"

    # serviceItemId taken from:
    # https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_2022/FeatureServer/0?f=pjson
    serviceItemId = "6c6743e1e4b444f6afcab9d9588f5d8f"

    # download_from_wfs(oa_wfs_url, layer_name)
    download_from_arcgis_online(serviceItemId, "data/oa_from_agol2.geojson")
