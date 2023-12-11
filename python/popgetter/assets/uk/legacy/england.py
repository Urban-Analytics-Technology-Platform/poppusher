#!/usr/bin/python3
from __future__ import annotations

import csv
import json
import subprocess
from collections import defaultdict

import pandas as pd

# import geopandas as gpd
from dagster_pipes import PipesContext, PipesMetadataValue, open_dagster_pipes
from downloader import download_from_arcgis_online

open_dagster_pipes()


def convert_geo_to_topo_json(geojson_file, topojson_file, new_layer_name, working_dir):
    """
    Converts a GeoJSON file to a TopoJSON file.
    """
    print("Running mapshaper")
    # TODO This hardcodes renaming the fields in a way that is not flexible. To Fix
    subprocess.check_call(
        [
            "mapshaper",
            "-i",
            geojson_file,
            "-rename-layers",
            new_layer_name,
            "-rename-fields",
            "ID=OA21CD",
            "-o",
            topojson_file,
        ],
        cwd=working_dir,
    )


def download_vehicle_ownership(census_url, working_dir):
    print("Retrieving Vehicle Ownership Census Data")
    # TODO This doesn't give programmatic access to the name of the downloaded file. This is a problem if/when the url's slug contains many parameters (where the parameter order is not guaranteed or if the parameter string is not a valid filename for all OSes)
    result = subprocess.check_call(["wget", "-N", census_url], cwd=working_dir)
    print(f"result = {result}")


def join_vehicle_ownership(output_areas_topojson_path, census_path, output_path):
    # Load the Output Areas TopoJSON
    print("Loading TopoJSON")
    with open(output_areas_topojson_path) as f:
        topojson = json.load(f)

    # Load the Vehicle Ownership Census Data
    # Per OA, scrape [cars_0, cars_1, cars_2, cars_3]
    data = defaultdict(lambda: [0, 0, 0, 0])
    with open(census_path) as f:
        for row in csv.DictReader(f):
            oa = row["Output Areas"]
            code = int(row["Car or van availability (5 categories) Code"])
            # Ignore "Does not apply" -- but TODO what does this mean?
            if code != -8:
                data[oa][code] = int(row["Observation"])

    # Add the Census to the TopoJSON
    print("Adding vehicle ownership to TopoJSON")
    missing = []
    for obj in topojson["objects"]["zones"]["geometries"]:
        props = obj["properties"]
        values = data[props["ID"]]
        # TODO Check it's not the default [0, 0, 0, 0]?
        if values == [0, 0, 0, 0]:
            missing.append(props["ID"])
        props["cars_0"] = values[0]
        props["cars_1"] = values[1]
        props["cars_2"] = values[2]
        props["cars_3"] = values[3]

    print(f"Missing car ownership data for {len(missing)} OAs: {missing}")

    # Save the TopoJSON
    print("Saving TopoJSON")
    with open(output_path, "w") as f:
        json.dump(topojson, f)


if __name__ == "__main__":
    WORKING_DIR = "data"

    # Ideally we would use the Output Areas as served by the ONS:
    # OUTPUT_AREAS_URL = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_2022/FeatureServer/"

    # The easiest way to get the Output Areas is to download them from ArcGIS Online.
    output_areas_serviceItemID = "6c6743e1e4b444f6afcab9d9588f5d8f"
    output_areas_geojson_path = f"{WORKING_DIR}/oa_from_agol.geojson"
    output_areas_topojson_path = f"{WORKING_DIR}/oa_from_agol.topojson"
    layer_name = "zones"

    download_from_arcgis_online(output_areas_serviceItemID, output_areas_geojson_path)
    convert_geo_to_topo_json(
        output_areas_geojson_path,
        output_areas_topojson_path,
        layer_name,
        working_dir=".",
    )

    # Vehicle Ownership

    # The WebUI to access the car ownership census data is here:
    # https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3/filter-outputs/a20437fb-ae7f-439b-bc91-de261335038b#get-data
    # This need to be downloaded manually.
    #
    # The csv can be downloaded from programatically from:
    CENSUS_URL = "https://static.ons.gov.uk/datasets/a20437fb-ae7f-439b-bc91-de261335038b/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv"
    census_path = f"{WORKING_DIR}/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv"
    combined_output_path = (
        f"{WORKING_DIR}/england_wales_oa_with_vehicle_ownership.topojson"
    )

    download_vehicle_ownership(CENSUS_URL, WORKING_DIR)
    join_vehicle_ownership(
        output_areas_topojson_path, census_path, combined_output_path
    )

    # Report back to Dagster (if running in Dagster)
    context = PipesContext.get()
    assert (
        context.asset_key
    )  # things like asset key are passed automatically and available in context
    # context.report_asset_materialization(metadata={"nrows": nrows}) # nrows was defined somewhere in the script

    census_df = pd.read_csv(census_path)

    context.report_asset_materialization(
        metadata={
            "census_num_records": len(census_df),  # Metadata can be any key-value pair
            "census_columns": PipesMetadataValue(
                type="md",
                raw_value="\n".join(
                    [f"- '`{col}`'" for col in census_df.columns.to_list()]
                ),
            ),
            "census_preview": PipesMetadataValue(
                type="md", raw_value=census_df.head().to_csv()
            ),
        }
    )
