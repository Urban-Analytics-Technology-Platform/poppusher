#!/bin/bash

set -e -x

mkdir -p data
cd data

wget -N https://ramp0storage.blob.core.windows.net/nationaldata-v2/GIS/OA_2011_Pop20.geojson
mapshaper -i OA_2011_Pop20.geojson -filter-fields OA11CD -rename-fields ID=OA11CD -o precision=0.001 uk_oa.topojson

# TODO E00017740 is broken
python3 << END
import json
f = open("uk_oa.topojson")
x = json.load(f)
obj = x["objects"]["OA_2011_Pop20"]
obj["geometries"] = [x for x in obj["geometries"] if x["type"] != None]
with open("uk_oa.topojson", "w") as f:
  json.dump(x, f)
END
