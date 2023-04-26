# popgetter

This is a scratch-space to experiment with coercing census data from different
places into a common format. See
<https://dabreegster.github.io/talks/census_pitch/slides.html>

## Notes

- dadroit is the best known way to poke at big JSON files

## Requirements

- Python
- mapshaper (See https://github.com/mbloch/mapshaper#installation)
- wget (not standard on Windows)
- ogr2ogr

# How it works

The top level is that:

- `england.py` download the data from the ONS
- `src/*.rs` converts the data into a common format



# Data

Currently:

- `OA_2011_Pop20.geojson` contains 227759 records
- `uk_oa.topojson` contains 171372 records

## Census

    # The WebUI to access the car ownership census data is here:
    # https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3/filter-outputs/a20437fb-ae7f-439b-bc91-de261335038b#get-data
    # This need to be downloaded manually.
    #
    # The csv can be downloaded using wget from this URL (though it is unclear if this URL will remain valid)):
    CENSUS_URL = "https://static.ons.gov.uk/datasets/a20437fb-ae7f-439b-bc91-de261335038b/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv"

    This has 944401 lines (including the header)
    944400 / 188880 = 5 categories per OA, which is what we expect.

    ```
    ogrinfo WFS:"https://dservices1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_/WFSServer?service=wfs&request=getcapabilities" OA_2021_EW_BGC
    ```

## Geometries

There are three different versions of the Output Areas (OAs) available from the ONS. By ad-hoc visual inspection, the Generalised Clipped EW (BGC) at least as good as the current data - but will review.

Each of these versions has 188880 records. This is consistent with the census data (see below). I presume from the filename that the OA are from 2011, and that's the reason for the discrepancy between the number of records in the census data and the geometry data.

- Output Areas (Dec 2021) Boundaries Generalised Clipped EW (BGC)
    - WebUI: https://geoportal.statistics.gov.uk/datasets/ons::output-areas-dec-2021-boundaries-generalised-clipped-ew-bgc
    - WFS https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_2022/FeatureServer/0

- Output Areas (Dec 2021) Boundaries Full Clipped EW (BFC)
    - WebUI: https://geoportal.statistics.gov.uk/datasets/ons::output-areas-dec-2021-boundaries-full-clipped-ew-bfc
    - WFS: https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Output_Areas_Dec_2021_Boundaries_Full_Clipped_EW_BFC_2022/FeatureServer

- Output Areas (2021) EW BFE (?Boundary Full Extent?)
    - webui:  https://geoportal.statistics.gov.uk/datasets/ons::output-areas-2021-ew-bfe/


There does not seem to be a straightforward way to programmatically download the data. The best (or least worst) way to use the WFS. However, there is a limit of 4000 records per request. It seems that OGR has a way of handling this:
https://gis.stackexchange.com/questions/422609/downloading-lots-of-data-from-wfs

Other options are:
* Manually download the data and save it somewhere.
* The file is downloaded from this URL, but there are authentication/session-cookie issues. Whilst it might be possible to hack this, it seems like a very fragile solution as no doubt the service has been designed to prevent this:
* https://stg-arcgisazurecdataprod1.az.arcgis.com/exportfiles-1559-17258/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_2022_-5131276949491721609.geojson?sv=2018-03-28&sr=b&sig=pH%2Fg7tbnYF5L%2BXB9lyHIs8kViwICm3YEIta7u9JT9Ug%3D&se=2023-04-21T14%3A34%3A44Z&sp=r

Note - also try:
```
ogrinfo WFS:"https://dservices1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/services/Output_Areas_Dec_2021_Boundaries_Generalised_Clipped_EW_BGC_/WFSServer?service=wfs&request=getcapabilities" OA_2021_EW_BGC
```

## Census data from browser download

The WebUI to access the car ownership census data is here:
https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3/filter-outputs/a20437fb-ae7f-439b-bc91-de261335038b#get-data
This need to be downloaded manually.

The csv can be downloaded using wget from this URL (though it is unclear if this URL will remain valid)):
CENSUS_URL = "https://static.ons.gov.uk/datasets/a20437fb-ae7f-439b-bc91-de261335038b/TS045-2021-3-filtered-2023-03-13T16:49:47Z.csv"

This has 944401 lines (including the header)
944400 / 188880 = 5 categories per OA, which is what we expect.

## Census data using the NOMIS API and UKCensusAPI

>     * keep a permalink to the extract? (let's see if https://www.nomisweb.co.uk/Query/GetFile?filename=2422351643080675.csv lasts over time)

I tried this on 2023/04/21 and got this error message:
```
Session timed out or not logged in: This page is only available when you query data on Nomis, you cannot bookmark it or directly link to it.
```
So unfortunately not that's not an option...

> I tried generating the API link through https://www.nomisweb.co.uk, but got an internal error of some sort.

I also tried this and got the same error message. However, in the [help](https://www.nomisweb.co.uk/api/v01/help), it states that there is a 25000 cell limit per request, which might be the issue we're hitting. It is possible to obtain an API key which increases the limit to 1 million cells. See https://github.com/virgesmith/UKCensusAPI#api-key

1M limit would be enough for the whole of England, for five categories per OA, but would not be sufficient for more complex census queries. In these cases, we could loop through batches of OA codes and then merge the results. Might be useful if we want to the be able to get lots of different data from NOMIS. It's not clear if the UKCensusAPI package can do this. If not then it might be appropriate to make a PR to that package rather then adding it here.


# Questions

Where does this pipeline need to be run? Are there any restrictions on the environment? (I note there is a comment above about processing bulk downloads in the browser).
