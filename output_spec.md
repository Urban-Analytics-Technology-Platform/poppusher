# Output specifications

Assets in `popgetter` are loosely grouped by country. When uploading these
assets to the cloud, the following directory structure should be obeyed.

## Top level

    /
    ├── countries.txt
    ├── be/
    ├── uk/
    ├── us/
    └── ...

The top level should contain:

- one directory per country (exemplified here by `be`, `uk`, `us`, ...)
- a `countries.txt` file which contains the names of these directories, one per
  line.

  In this case, the contents of the `countries.txt` file would be:

      be
      uk
      us

## Country subdirectories

    /be/
    ├── metric_metadata.parquet
    ├── source_data_releases.parquet
    ├── data_publishers.parquet
    ├── country_metadata.parquet
    ├── metrics/
    │   ├── {metric_filename_a}.parquet
    │   ├── {metric_filename_b}.parquet
    │   └── ...
    └── geometries/
        ├── {geo_filename_a}.fgb
        ├── {geo_filename_a}.parquet
        ├── {geo_filename_b}.fgb
        ├── {geo_filename_b}.parquet
        └── ...

Each country subdirectory should contain parquet files in specified filepaths
with tabulated metadata:

- A `metric_metadata.parquet` file containing a serialised list of
  `MetricMetadata`
- A `source_data_releases.parquet` file containing a serialised list of
  `SourceDataRelease`
- A `data_publishers.parquet` file containing a serialised list of
  `DataPublisher`
- A `country_metadata.parquet` file containing a serialised list of
  `CountryMetadata` (most countries should only have one entry, but there may be
  more than one because of entities like the UK)

All the metrics themselves should be placed in the `metrics` subdirectory. These
metrics must be dataframes with the appropriate geoIDs stored in a `GEO_ID`
column.
(This can just be an ordinary column rather than an index column, since Polars does not have the concept of an index column.)
These dataframes are then serialised as parquet files, and can be given any
filename, as the `MetricMetadata` struct should contain the filepath to them.

Likewise, geometries should be placed in the `geometries` subdirectory. Each set
of geometries should consist of two files, with the same filename stem and
different extensions:

- `{filename}.fgb` - a FlatGeobuf file with the geoIDs stored in the `GEO_ID`
  column
- `{filename}.parquet` - a serialised dataframe storing the names of the
  corresponding areas. This dataframe must have:

  - a `GEO_ID` column which corresponds exactly to those in the FlatGeobuf file.
  - one or more other columns, whose names are
    [lowercase ISO 639-3 chdes](https://iso639-3.sil.org/code_tables/639/data),
    and contain the names of each region in those specified languages.

  For example, the parquet file corresponding to the Belgian regions (with
  made-up geoIDs) may look like:

  | GEO_ID | nld                            | fra                          | deu                       |
  | ------ | ------------------------------ | ---------------------------- | ------------------------- |
  | 1      | Vlaams Gewest                  | Région flamande              | Flämische Region          |
  | 2      | Waals Gewest                   | Région wallonne              | Wallonische Region        |
  | 3      | Brussels Hoofdstedelijk Gewest | Région de Bruxelles-Capitale | Region Brüssel-Hauptstadt |