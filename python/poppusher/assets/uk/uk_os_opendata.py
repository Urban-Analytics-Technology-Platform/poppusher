from __future__ import annotations

from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
import requests
from dagster import (
    AssetIn,
    AssetOut,
    MarkdownMetadataValue,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)
from slugify import slugify

from poppusher.utils import (
    SourceDataAssumptionsOutdated,
    StagingDirResource,
    extract_main_file_from_zip,
    get_staging_dir,
    markdown_from_plot,
)

all_datasets = [
    {
        "Description": "1:250 000 Scale Colour Raster",
        "download_url": "https://api.os.uk/downloads/v1/products/250kScaleColourRaster/downloads?area=GB&format=TIFF-LZW&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/250kScaleColourRaster",
    },
    {
        "Description": "Boundary-Line",
        "download_url": "https://api.os.uk/downloads/v1/products/BoundaryLine/downloads?area=GB&format=GeoPackage&redirect",
        # "https://api.os.uk/downloads/v1/products/BoundaryLine/downloads?area=GB&format=Vector+Tiles&subformat=%28MBTiles%29&redirect"
        "landpage_url": "https://osdatahub.os.uk/downloads/open/BoundaryLine",
    },
    {
        "Description": "Code-Point® Open",
        "download_url": "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=GeoPackage&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/CodePointOpen",
    },
    {
        "Description": "GB Overview Maps",
        "download_url": "https://api.os.uk/downloads/v1/products/GBOverviewMaps/downloads?area=GB&format=GeoTIFF&subformat=Full+Colour&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/GBOverviewMaps",
    },
    {
        "Description": "MiniScale®",
        "download_url": "https://api.os.uk/downloads/v1/products/MiniScale/downloads?area=GB&format=Zip+file+%28containing+EPS%2C+Illustrator+and+TIFF-LZW%29&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/MiniScale",
    },
    {
        "Description": "OS Open Built Up Areas",
        "download_url": "https://api.os.uk/downloads/v1/products/BuiltUpAreas/downloads?area=GB&format=GeoPackage&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/BuiltUpAreas",
    },
    {
        "Description": "OS Open Greenspace",
        "download_url": "https://api.os.uk/downloads/v1/products/OpenGreenspace/downloads?area=GB&format=GeoPackage&redirect",
        # "https://api.os.uk/downloads/v1/products/OpenGreenspace/downloads?area=GB&format=Vector+Tiles&subformat=%28MBTiles%29&redirect"
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenGreenspace",
    },
    {
        "Description": "OS Open Linked Identifiers",
        # "Many download_urlS!"
        "download_url": None,
        "landpage_url": "https://osdatahub.os.uk/downloads/open/LIDS",
    },
    {
        "Description": "OS Open Names",
        "download_url": "https://api.os.uk/downloads/v1/products/OpenNames/downloads?area=GB&format=GeoPackage&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenNames",
    },
    {
        "Description": "OS Open Rivers",
        "download_url": "https://api.os.uk/downloads/v1/products/OpenRivers/downloads?area=GB&format=GeoPackage&redirect",
        # "https://api.os.uk/downloads/v1/products/OpenRivers/downloads?area=GB&format=Vector+Tiles&subformat=%28MBTiles%29&redirect"
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenRivers",
    },
    {
        "Description": "OS Open Roads",
        "download_url": "https://api.os.uk/downloads/v1/products/OpenRoads/downloads?area=GB&format=GeoPackage&redirect",
        # "https://api.os.uk/downloads/v1/products/OpenRoads/downloads?area=GB&format=Vector+Tiles&subformat=%28MBTiles%29&redirect"
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenRoads",
    },
    {
        "Description": "OS Open TOID",
        # "Download per OSGB Grid Tile {eg 100km x 100km}"
        "download_url": None,
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenTOID",
    },
    {
        "Description": "OS Open UPRN",
        "download_url": "https://api.os.uk/downloads/v1/products/OpenUPRN/downloads?area=GB&format=GeoPackage&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenUPRN",
    },
    {
        "Description": "OS Open USRN",
        "download_url": "https://api.os.uk/downloads/v1/products/OpenUSRN/downloads?area=GB&format=GeoPackage&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenUSRN",
    },
    {
        "Description": "OS Open Zoomstack",
        "download_url": "https://api.os.uk/downloads/v1/products/OpenZoomstack/downloads?area=GB&format=GeoPackage&redirect",
        # "https://api.os.uk/downloads/v1/products/OpenZoomstack/downloads?area=GB&format=Vector+Tiles&subformat=%28MBTiles%29&redirect"
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenZoomstack",
    },
    {
        "Description": "OS OpenMap - Local",
        "download_url": "https://api.os.uk/downloads/v1/products/OpenMapLocal/downloads?area=GB&format=GeoPackage&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/OpenMapLocal",
    },
    {
        "Description": "OS Terrain® 50",
        "download_url": "https://api.os.uk/downloads/v1/products/Terrain50/downloads?area=GB&format=GeoPackage&redirect",
        # "https://api.os.uk/downloads/v1/products/Terrain50/downloads?area=GB&format=Vector+Tiles&subformat=%28MBTiles%29&redirect"
        "landpage_url": "https://osdatahub.os.uk/downloads/open/Terrain50",
    },
    {
        "Description": "OS VectorMap® District",
        "download_url": "https://api.os.uk/downloads/v1/products/VectorMapDistrict/downloads?area=GB&format=GeoPackage&redirect",
        "landpage_url": "https://osdatahub.os.uk/downloads/open/VectorMapDistrict",
    },
]

_all_datasets = []
for ds_dict in all_datasets:
    ds_dict["name"] = slugify(ds_dict["Description"], separator="_")
    _all_datasets.append(ds_dict)

all_datasets = _all_datasets

dataset_names = [dataset["name"] for dataset in all_datasets]


boundary_line_layers = [
    "boundary_line_ceremonial_counties",
    "boundary_line_historic_counties",
    "community_ward",
    "country_region",
    "county",
    "county_electoral_division",
    "district_borough_unitary",
    "district_borough_unitary_ward",
    "english_region",
    "greater_london_const",
    "high_water",
    "historic_european_region",
    "parish",
    "polling_districts_england",
    "scotland_and_wales_const",
    "scotland_and_wales_region",
    "unitary_electoral_division",
    "westminster_const",
]


@asset(description="List of all OS OpenData datasets")
def os_opendata_list():
    # Functionally this is just a passthrough, but it's useful to have an asset which
    # provides a "root" for all of the OS OpenData datasets.
    return pd.DataFrame(all_datasets)


@multi_asset(
    outs={name: AssetOut(key=name, is_required=False) for name in dataset_names},
    can_subset=True,
)
def uk_os_opendata(context, os_opendata_list):
    for ds_name in context.selected_output_names:
        ds_details = os_opendata_list[os_opendata_list["name"] == ds_name].to_dict(
            orient="records"
        )[0]
        yield Output(value=ds_details, output_name=ds_name)


@multi_asset(
    ins={"boundary_line": AssetIn("boundary_line")},
    outs={name: AssetOut(is_required=False) for name in boundary_line_layers},
    can_subset=True,
    required_resource_keys={"staging_res"},
)
def uk_os_opendata_boundary_line(context, boundary_line):
    # "Boundary-Line"
    # row = uk_os_opendata_list[uk_os_opendata_list["name"] == "Boundary-Line"]
    # download_url = row["download_url"].values[0]
    download_url = boundary_line["download_url"]
    staging_res = context.resources.staging_res

    archive_path_guess = "Data/file.gpkg"
    gpkg_path = download_with_redirect(
        context, staging_res, download_url, archive_path_guess
    )

    # TODO: Replace with:
    # https://pyogrio.readthedocs.io/en/latest/introduction.html#list-available-layers
    all_layers = set(fiona.listlayers(gpkg_path))

    expected_layers = set(boundary_line_layers)

    # Verify that the downloaded GeoPackage contains exactly the expected layers
    if all_layers != expected_layers:
        missing_layers = expected_layers - all_layers
        err_msg = ""
        if len(missing_layers) > 0:
            err_msg += (
                "Downloaded Boundary-Line GeoPackage is missing some expected layers:\n"
            )
            err_msg += "".join([f"- {layer}\n" for layer in missing_layers])

        extra_layers = all_layers - expected_layers

        if len(extra_layers) > 0:
            err_msg += (
                "Downloaded Boundary-Line GeoPackage has some unexpected layers:\n"
            )
            err_msg += "".join([f"- {layer}\n" for layer in extra_layers])

        err_msg += "Please check the download and update this DAG accordingly."
        raise SourceDataAssumptionsOutdated(err_msg)

    for lyr_name in context.selected_output_names:
        gdf = get_layer_from_gpkg(context, gpkg_path, lyr_name)
        yield Output(value=gdf, output_name=lyr_name)


def get_layer_from_gpkg(context, gpkg_path, layer_name):
    """
    Helper function to read a layer from a GeoPackage using pyogrio.
    """
    lyr_gdf = gpd.read_file(
        gpkg_path,
        layer=layer_name,
        # engine="pyogrio"
    )

    metadata = {
        "title": layer_name,
        "num_records": len(lyr_gdf),  # Metadata can be any key-value pair
        "columns": MetadataValue.md(
            "\n".join([f"- '`{col}`'" for col in lyr_gdf.columns.to_list()])
        ),
        "preview": MetadataValue.md(
            lyr_gdf.loc[:, lyr_gdf.columns != "geometry"].head().to_markdown()
        ),
    }

    # Only plot the layer if it's small enough
    if len(lyr_gdf) < 10000:
        # Plot and convert the image to Markdown to preview it within Dagster
        ax = lyr_gdf.plot(legend=False)
        ax.set_title(layer_name)
        md_plot = markdown_from_plot()
        # Force the type to be MarkdownMetadataValue
        # else sometimes Dagster will try to interpret it as a tuple for some unknown reason
        mdv: MarkdownMetadataValue = MetadataValue.md(md_plot)
        metadata["plot"] = mdv
    else:
        metadata["plot"] = MetadataValue.md(
            f"Too many features in the layer '{layer_name}' to plot"
        )

    context.add_output_metadata(metadata=metadata, output_name=layer_name)

    return lyr_gdf


def download_with_redirect(
    context,
    staging_res: StagingDirResource,
    source_download_url,
    source_archive_file_path,
):
    expected_extension = Path(source_archive_file_path).suffix

    with get_staging_dir(context, staging_res) as staging_dir_str:
        staging_path = Path(staging_dir_str)
        temp_file = staging_path / "temp_file.zip"

        # Given we're using a staging directory, we can assume that we only need to download the file if it doesn't exist
        if not temp_file.exists():
            # Using a custom header here to avoid a javascript redirect
            # https://stackoverflow.com/a/50111203
            headers = {"User-Agent": "poppusher"}

            with temp_file.open(mode="wb") as f, requests.get(
                source_download_url, headers=headers, allow_redirects=True, stream=True
            ) as r:
                for chunk in r.iter_content(chunk_size=(16 * 1024 * 1024)):
                    f.write(chunk)

        # unzip the file
        return extract_main_file_from_zip(temp_file, staging_path, expected_extension)
