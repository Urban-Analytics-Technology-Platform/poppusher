## Singapore

First of all: this is not my work, but there is a really nice collated list of datasets here https://ual.sg/post/2020/06/24/guide-for-open-urban-data-in-singapore/

**Granularity**

Most Singapore data is reported country-wide. Only some datasets are broken down by 'planning areas' and 'subzones'.

See the datasets titled *'Geographical ...'* in [Statistical Release 2](https://www.singstat.gov.sg/publications/reference/cop2020/cop2020-sr2/census20_stat_release2), as well as the [search results for 'planning area' on data.gov.sg](https://legacy.data.gov.sg/dataset?q=planning+area).

**Caveats**

Many datasets only cover Singapore citizens, or citizens and permanent residents. Always check the dataset metadata for clarification.

**Licensing terms**

[Singapore Open Data License](https://legacy.data.gov.sg/open-data-licence)

TL;DR: OK to store your own copy with attribution, and as long as you don't pretend your app is officially endorsed

**Programmatic / API**

- [Data.gov.sg](https://guide.data.gov.sg/developers/api-v1) A lot of things here tell you to go to SingStat anyway. :upside_down_face: Note that Census data isn't available here (lol)

  - Managed to get geometries!!
  - Get the 'resource ID' at the end of the url (e.g. [this one](https://legacy.data.gov.sg/dataset/master-plan-2019-subzone-boundary-no-sea?resource_id=84b62d90-c1b7-4ada-acfc-f5874b5fd945) - but make sure the IDs are being taken from the legacy website as that is what works with the current API version)
  - `curl https://data.gov.sg/api/action/resource_show?id=84b62d90-c1b7-4ada-acfc-f5874b5fd945`
  - Inside the `url` field there is a direct link to the GeoJSON   

 - [SingStat table builder](https://tablebuilder.singstat.gov.sg/view-api/for-developers)
 
   - 2020 Census - Resident Population broken down by Planning Area/Subzone, Sex, and Age Group: https://tablebuilder.singstat.gov.sg/table/CT/17560
     - `curl https://tablebuilder.singstat.gov.sg/api/table/tabledata/17560`
 
 - Urban Redevelopment Authority (for land use) https://www.ura.gov.sg/maps/api/


**Manual downloads**

 - [Census 2020 Statistical Release 1](https://www.singstat.gov.sg/publications/reference/cop2020/cop2020-sr1/census20_stat_release1)
 - [Census 2020 Statistical Release 2](https://www.singstat.gov.sg/publications/reference/cop2020/cop2020-sr2/census20_stat_release2)
 - Data.gov.sg
