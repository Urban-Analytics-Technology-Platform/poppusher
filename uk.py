#!/usr/bin/python3

from collections import defaultdict
import csv
import json
import os
import subprocess

def getTopoJsonGeometry():
    try:
        os.mkdir("data")
    except FileExistsError:
        pass

    # TODO Don't use OA GeoJSON from SPC
    subprocess.run(["wget", "-N", "https://ramp0storage.blob.core.windows.net/nationaldata-v2/GIS/OA_2011_Pop20.geojson"], cwd="data")
    # Convert GeoJSON to TopoJSON for space savings
    subprocess.run(["mapshaper", "-i", "OA_2011_Pop20.geojson", "-filter-fields", "OA11CD", "-rename-fields", "ID=OA11CD", "-o", "precision=0.001", "uk_oa.topojson"], cwd="data")

    # TODO E00017740 and maybe others are broken. Filter out nulls.
    f = open("uk_oa.topojson")
    x = json.load(f)
    obj = x["objects"]["OA_2011_Pop20"]
    obj["geometries"] = [x for x in obj["geometries"] if x["type"] != None]

    return x

# Requires https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3/filter-outputs/a20437fb-ae7f-439b-bc91-de261335038b#get-data to be downloaded manually.
def joinVehicleOwnership(topojson):
    # Per OA, scrape [cars_0, cars_1, cars_2, cars_3]
    data = defaultdict(lambda: [0, 0, 0, 0])
    with open("/home/dabreegster/Downloads/TS045-2021-3-filtered-2023-03-13T16 49 47Z.csv") as f:
        for row in csv.DictReader(f):
            oa = row['Output Areas']
            code = int(row['Car or van availability (5 categories) Code'])
            # Ignore "Does not apply" -- but TODO what does this mean?
            if code != -8:
                data[oa][code] = int(row['Observation'])

    # Add to the TopoJSON
    missing = []
    for obj in topojson["objects"]["OA_2011_Pop20"]["geometries"]:
        props = obj["properties"]
        values = data[props["ID"]]
        # TODO Check it's not the default [0, 0, 0, 0]?
        if values == [0, 0, 0, 0]:
            missing.append(props["ID"])
        props["cars_0"] = values[0]
        props["cars_1"] = values[1]
        props["cars_2"] = values[2]
        props["cars_3"] = values[3]

    print(f"Missing car ownership data: {missing}")


if __name__ == "__main__":
    topojson = None
    if False:
        topojson = getTopoJsonGeometry()
    else:
        with open("data/uk_oa.topojson") as f:
            topojson = json.load(f)

    joinVehicleOwnership(topojson)

    with open("data/uk_oa.topojson", "w") as f:
      json.dump(topojson, f)
