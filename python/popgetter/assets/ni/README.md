# Northern Ireland

## Summary

Census 2021 is available from
[https://build.nisra.gov.uk](https://build.nisra.gov.uk/en/).

The processing pipeline involves the following steps, achieved by implementing
the [`Country`](../country.py) base class:

- Retrieve the geography data and outputs with standard geometry formats
  (`geometry` asset)
- Generate metadata associated with country, data publisher and source data
  releases (`country_metadata`, `data_publisher` and `source_data_releases`
  assets)
- Generate a catalog by identifying all tables
  [available](https://build.nisra.gov.uk/en/standard) (`catalog` asset)
- Read table metadata and census tables, across different geography levels,
  currently for Data Zone 2021, Super Data Zone 2021 and Local Government
  District 2014 (`census_tables` and `source_metric_metadata` assets)
- Process census tables into metrics per geography ID and any other pre-defined
  derived metrics (`metrics` asset)
