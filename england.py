#!/usr/bin/python3

from collections import defaultdict
import csv
import json
import os
import subprocess

def getTopoJsonGeometry(output_areas_url):
    try:
        os.mkdir("data")
    except FileExistsError:
        pass

    # TODO Don't use OA GeoJSON from SPC
    print("Retrieving Output Areas Geometry")
    subprocess.run(["wget", "-N", output_areas_url], cwd="data")

    # Convert GeoJSON to TopoJSON for space savings. Filter to England.
    print("Running mapshaper")
    subprocess.run(["mapshaper", "-i", "OA_2011_Pop20.geojson", "-filter", "OA11CD.startsWith('E')", "-filter-fields", "OA11CD", "-rename-fields", "ID=OA11CD", "-o", "uk_oa.topojson"], cwd="data")

    # TODO E00017740 and maybe others are broken. Filter out nulls.
    print("Filtering broken results")
    f = open("data/uk_oa.topojson")
    x = json.load(f)
    # Rename the layer. Might be possible with mapshaper.
    x["objects"]["zones"] = x["objects"]["OA_2011_Pop20"]
    del x["objects"]["OA_2011_Pop20"]
    obj = x["objects"]["zones"]
    obj["geometries"] = [x for x in obj["geometries"] if x["type"] != None]

    return x

# Requires https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3/filter-outputs/a20437fb-ae7f-439b-bc91-de261335038b#get-data to be downloaded manually.
#          https://static.ons.gov.uk/datasets/a20437fb-ae7f-439b-bc91-de261335038b/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv#get-data
def joinVehicleOwnership(topojson, census_url):
    # Per OA, scrape [cars_0, cars_1, cars_2, cars_3]

    print("Retrieving Vehicle Ownership Census Data")
    subprocess.run(["wget", "-N", census_url], cwd="data")

    data = defaultdict(lambda: [0, 0, 0, 0])
    with open("data/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv") as f:
        for row in csv.DictReader(f):
            oa = row['Output Areas']
            code = int(row['Car or van availability (5 categories) Code'])
            # Ignore "Does not apply" -- but TODO what does this mean?
            if code != -8:
                data[oa][code] = int(row['Observation'])

    # Add to the TopoJSON
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


if __name__ == "__main__":
    # The WebUI to access the car ownership census data is here:
    # https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3/filter-outputs/a20437fb-ae7f-439b-bc91-de261335038b#get-data
    # This need to be downloaded manually.
    #
    # The csv can be downloaded from programatically from:
    CENSUS_URL = "https://static.ons.gov.uk/datasets/a20437fb-ae7f-439b-bc91-de261335038b/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv"


    # TODO find a better source for the Output Areas GeoJSON from SPC
    # Ideally we would use the OA GeoJSON from ONS, but this is not working yet (see downloader::download_from_wfs)
    # OUTPUT_AREAS_URL = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_2022/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"

    # For not we use this from SPC
    OUTPUT_AREAS_URL = "https://ramp0storage.blob.core.windows.net/nationaldata-v2/GIS/OA_2011_Pop20.geojson"

    topojson = None
    if True:
        topojson = getTopoJsonGeometry(OUTPUT_AREAS_URL)
    else:
        print("Loading TopoJSON")
        with open("data/uk_oa.topojson") as f:
            topojson = json.load(f)

    joinVehicleOwnership(topojson, CENSUS_URL)

    print("Writing TopoJSON")
    with open("data/uk_oa.topojson", "w") as f:
      json.dump(topojson, f)
