# Defining a new country

## Overview

In general, each self-contained set of census data will correspond to a single
territory, which we loosely refer to as a 'country'. Each country in popgetter
will have a group of assets that it publishes. To add a new country to
popgetter, you will need to:

- Create a new subdirectory `python/popgetter/assets/{COUNTRY_ID}`.
  `{COUNTRY_ID}` here is a lowercase, unique identifier for the country. (In
  principle, we would like this to correspond to the
  `popgetter.metadata.CountryMetadata.id` computed field: thus, for an actual
  country, this will be its ISO 3166-1 alpha-3 code (e.g. 'bel' for Belgium);
  and for a subdivision of a country, this will be its ISO 3166-2 code. This is
  not yet the case.)

- Inside `python/popgetter/assets/__init__.py`, import the new country
  subdirectory as a module and add this module to the list of `countries`. This
  allows Dagster to detect the assets belonging to the new country.

- Define the requisite assets inside the country subdirectory, as will be
  described below. Note that you can structure the code inside the country
  subdirectory however you like (e.g. across multiple files), as Dagster will
  load all the assets in that subdirectory.

## Required assets

There are, fundamentally, five assets which must be defined for each country.
Their return types are fixed, but they can have any input types (i.e. you can
construct the asset graph in any way you like).

Three of these are metadata assets:

- **Country metadata:** an asset which returns
  `popgetter.metadata.CountryMetadata` object(s).
- **Data publisher metadata:** an asset which returns
  `popgetter.metadata.DataPublisher` object(s).
- **Source release metadata:** an asset which returns
  `popgetter.metadata.SourceDataRelease` object(s).

These metadata assets can return either a single object, a list of objects, or a
dictionary where the values are objects. This flexibility makes it easier to
construct dependencies between assets in Dagster depending on your needs.

- **Geometries:** an asset which returns a list of
  `popgetter.cloud_outputs.GeometryOutput` objects

A `GeometryOutput` is essentially a named tuple of
`popgetter.metadata.GeometryMetadata` (which provides metadata about the
geometry), a `geopandas.GeoDataFrame` object (which contains geoIDs and the
geometries themselves), and a `pandas.DataFrame` object (which contains geoIDs
and the names of the regions).

Note that the GeoDataFrame must _only_ contain the geometries and the geoIDs,
and the DataFrame must _only_ contain the geoIDs and the names of the regions.
Additionally, the geoID column in both of these must be named `GEO_ID`; and the
column names in the DataFrame must correspond to
[lowercase ISO 639-3 codes](https://iso639-3.sil.org/code_tables/639/data).

(By the way: instead of using the literal string `"GEO_ID"`, you should use
`popgetter.metadata.COL.GEO_ID.value` to ensure that the column name is always
correct.)

- **Metrics:** an asset which returns a list of
  `popgetter.cloud_outputs.MetricsOutput` objects

One `MetricsOutput` in turn comprises a list of
`popgetter.metadata.MetricMetadata` classes (which provides metadata about the
metric), and a `pandas.DataFrame` object (which contains the metric data). Each
element of the metadata list will correspond to one of the columns in the
DataFrame. The DataFrame must also contain a `GEO_ID` column, which contains the
geoIDs that correspond to the geometries.

This asset returns a _list_ of `MetricsOutput` objects because each of the the
individual outputs will be serialised to a separate parquet file. The location
of this parquet file is specified as part of the `MetricMetadata` object.

(Note that because a `MetricMetadata` object includes an ID for the
`SourceDataRelease` that it corresponds to, which _in turn_ contains an ID for
the `GeometryMetadata`, each set of metrics can be tied to one geometry level.)

## Publishing the assets

Defining the assets and importing them should allow you to view the asset graph
in the Dagster UI and materialise the assets. When the assets are materialised,
Dagster will serialise their return values by pickling them and storing them
inside the `$DAGSTER_HOME/storage` directory. However, these files are not
suitable for consumption by downstream tasks such as the popgetter CLI: the CLI
expects data and metadata to be provided in a specific format (see
[Output structure](output_structure.md)).

In the popgetter library, the pipeline which publishes (meta)data in the correct
format is constructed using
[sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors).
These sensors monitor a list of specified assets for materialisations, and will
publish their return values in the correct format when new materialisations are
observed. (As a bonus, if any of your assets do not have the correct return
types or do not satisfy any of the extra stipulations above, the sensor will
raise an error.)

If the `ENV` environment variable is set to `prod`, the sensors will publish the
data to an Azure blob storage container; otherwise, the data will be published
to `$DAGSTER_HOME/cloud_outputs`. To publish to Azure you will also need to set
the additional environment variable `SAS_TOKEN`.

To attach your newly defined assets to the sensors, all you need to do is to
import the following decorators:

```python
from popgetter.cloud_outputs import (
    send_to_metadata_sensor,
    send_to_geometry_sensor,
    send_to_metrics_sensor,
)
```

and decorate your assets with these. The three metadata assets will use the
`send_to_metadata_sensor` decorator, and likewise for the others. Note that this
decorator expects an asset as an input, so the decorator must be applied as the
outermost decorator, i.e. _above_ Dagster's `@asset` decorator. For example:

```python
@send_to_metadata_sensor
@asset(...)
def country_metadata():
    return CountryMetadata(...)
```

## `Country` base class

To simplify the process of defining the assets and the associated relationships
between them, we provide a `Country` base class which you can inherit from.
These abstract away most of Dagster's implementation details, and mean that you
only need to write the actual Python functions to process the data. For example,
instead of the `country_metadata` asset above, you could write:

```python
from popgetter.assets.country import Country


class MyCountry(Country):
    def _country_metadata(self, context):
        return CountryMetadata(...)


my_country = MyCountry()
country_metadata_asset = my_country.create_country_metadata()
```

The `create_country_metadata` method will generate a Dagster asset and register
it with the metadata sensor for you. The reason why this line is necessary is
that Dagster can only detect assets which are defined at the top level of any
module: so, calling this method binds an asset to a top-level definition which
can then be picked up.

For an example of this, see the implementation of Northern Ireland data in
`python/popgetter/assets/ni`.

Naturally, the implementation of this base class makes some assumptions about
the structure of the data and the relationships between them. We have found
these to be applicable across multiple countries we have worked with. However,
if these are not suitable for your data, you can still define the assets
manually as shown above! It is also possible to override part of the base class
with manual asset implementations to include variations from the structure
assumed by the base class.
