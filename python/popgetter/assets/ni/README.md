# Northern Ireland

## Summary

Census 2021 is available from
[https://build.nisra.gov.uk](https://build.nisra.gov.uk/en/).

The processing pipeline involves the following steps:

- Gets the corresponding geography files and outputs as standard geometry
  formats
- Generates metadata associated with Northern Ireland and data releases
- Generates a catalog by identifying all tables
  [available](https://build.nisra.gov.uk/en/standard).
- Read table metadata and census tables, across different geography levels
  (currently only Data Zone 2021 and Super Data Zone 2021)
- Construct a set of pre-defined derived metrics
