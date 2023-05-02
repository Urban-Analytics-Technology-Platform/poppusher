# popgetter

This is a scratch-space to experiment with coercing census data from different
places into a common format. See
<https://dabreegster.github.io/talks/census_pitch/slides.html>

## Notes

- dadroit is the best known way to poke at big JSON files

## Requirements

- *NIX-like environment (Linux, MacOS, WSL on Windows)
- Python
- mapshaper (See https://github.com/mbloch/mapshaper#installation)
- wget (not standard on Windows)

### Optional dependency - ArcGIS API for Python
To be able to download data from ArcGIS Online, you need to install ESRI's "ArcGIS API for Python".
```
pip install -r requirements-non-foss.txt
```

TODO: The ArcGIS API for Python  docs claim that it is possible to [install it with minimal dependencies](https://developers.arcgis.com/python/guide/anaconda#install-with-minimum-dependencies). However, this did not work exactly as stated in the docs. At some point, it might be worth looking at what the minimal dependencies are for our use case.

### Optional dependency - OGR

To be able to download data from WFS (Web Feature Service), you need [GDAL/OGR](https://gdal.org/download.html):

- ogr2ogr


##  Data (England and Wales)

### Geometries (Output Areas)

There are three different versions of the Output Areas (OAs) available from the ONS. By ad-hoc visual inspection, the Generalised Clipped EW (BGC) at least as good as the current data - but will review.

Each of these versions has 188880 records. This is consistent with the census data (see below).

- [Output Areas (Dec 2021) Boundaries Generalised Clipped EW (BGC)](https://geoportal.statistics.gov.uk/datasets/ons::output-areas-dec-2021-boundaries-generalised-clipped-ew-bgc)
- [Output Areas (Dec 2021) Boundaries Full Clipped EW (BFC)](https://geoportal.statistics.gov.uk/datasets/ons::output-areas-dec-2021-boundaries-full-clipped-ew-bfc)
- [Output Areas (2021) EW BFE (?Boundary Full Extent?)](https://geoportal.statistics.gov.uk/datasets/ons::output-areas-2021-ew-bfe/)

### Census Information

Numerous potential queries can be made to the census data. For now we are focused on car ownership.

The WebUI to access the car ownership census data is here:
https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3/filter-outputs/a20437fb-ae7f-439b-bc91-de261335038b#get-data
This need to be downloaded manually.

The csv can be downloaded using wget from this URL (though it is unclear if this URL will remain valid)):
CENSUS_URL = "https://static.ons.gov.uk/datasets/a20437fb-ae7f-439b-bc91-de261335038b/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv"

This has 944401 lines (including the header)
944400 / 188880 = 5 categories per OA, which is what we expect.

### Census data from browser download

The WebUI to access the car ownership census data is here:
https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3/filter-outputs/a20437fb-ae7f-439b-bc91-de261335038b#get-data
This need to be downloaded manually.

The csv can be downloaded using wget from this URL (though it is unclear if this URL will remain valid)):
CENSUS_URL = "https://static.ons.gov.uk/datasets/a20437fb-ae7f-439b-bc91-de261335038b/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv"

This has 944401 lines (including the header)
944400 / 188880 = 5 categories per OA, which is what we expect.

### Census data using the NOMIS API and UKCensusAPI

TBC:

> I tried generating the API link through https://www.nomisweb.co.uk, but got an internal error of some sort.

I also tried this and got the same error message. However, in the [help](https://www.nomisweb.co.uk/api/v01/help), it states that there is a 25000 cell limit per request, which might be the issue we're hitting. It is possible to obtain an API key which increases the limit to 1 million cells. See https://github.com/virgesmith/UKCensusAPI#api-key

1M limit would be enough for the whole of England, for five categories per OA, but would not be sufficient for more complex census queries. In these cases, we could loop through batches of OA codes and then merge the results. Might be useful if we want to the be able to get lots of different data from NOMIS. It's not clear if the UKCensusAPI package can do this. If not then it might be appropriate to make a PR to that package rather then adding it here.
