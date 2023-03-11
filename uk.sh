#!/bin/bash

set -e -x

mkdir -p data
cd data

wget -N https://ramp0storage.blob.core.windows.net/nationaldata-v2/GIS/OA_2011_Pop20.geojson
mapshaper -i OA_2011_Pop20.geojson -filter-fields OA11CD -rename-fields ID=OA11CD -o precision=0.001 uk_oa.topojson
