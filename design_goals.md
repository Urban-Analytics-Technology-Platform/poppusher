# What is the purpose of popgetter

The purpose of popgetter is to provide a convenience tool for downloading and
cleaning demographic data. It can be used to maintain a known "good" copy of the
data in a common shared location.

Downstream applications that need to operate in multiple countries and require
certain demographic data can use popgetter to prepare data from multiple
country-specific data sources into a common format.

It is not intended that downstream applications would directly embed popgetter.

- popgetter is invoked locally, via CLI for one or more countries and datasets,
  saving the processed files locally.
- popgetter is run independently, for all implemented countries and datasets,
  saving its processed files into a stable, shared, accessible, common location
  (eg AWS S3, Azure storage bucket etc). Downstream applications can simply
  retrieve data from this location, trusting that popgetter as prepared them
  appropriately.

It is not envisioned that downstream applications would use popgetter to
retrieve live data.

Wherever possible popgetter avoids being opinionated on the data itself,
allowing downstream applications to decide how to interpret subtleties in the
data.

# What does popgetter do?

- For each country implemented, popgetter downloads and unpacks;
  - Census boundaries for the highest available spatial resolution
  - Population statistics including age/sex breakdown and car ownership. (Other
    parameters may be added in due course).
- Attempts to join the geometry data with the population statistics, alerting
  the user to join errors, missing data etc.
- Exports the joined data into a common file format.
- Implement sensible caching, idempotency and integrity checks. Where possible
  detect when source data has been updated and only download as required.
- (Optionally) rename columns to match a common schema
- (Optionally), reproject the to a common, globally applicable CRS (eg WGS1984)
- (Optionally) uploaded the exported data to a common shared location

# What does popgetter not do?

The following are not goals for v1.0:

- Spatial re-sampling and interpolation
- Temporal interpolation
- Process demographic data in a manner that supports international comparisons.
