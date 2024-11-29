# poppusher CLI

For countries which have been ported to Dagster, the downloads can be invoked
via the [Dagster CLI](https://docs.dagster.io/_apidocs/cli).

### Run a single job

Typically there is a single job per country. For Belgium, the job is called
`job_be`:

```bash
DAGSTER_HOME=$PWD/persist dagster job execute -m poppusher --job job_be
```

### Run a single asset

Within a country, there are likely to be multiple assets. To materialize a
single asset (and any required dependencies). The command gets the number of
cars per census sector in Belgium.

```bash
DAGSTER_HOME=$PWD/persist dagster asset materialize -m poppusher --select be/get_car_per_sector
```

(Typically we organise assets with the country code as a prefix, to ensure that
asset names are unique).

### Run everything

(The quotes around the `*` are required to prevent it being interpreted by the
shell.)

```bash
DAGSTER_HOME=$PWD/persist dagster asset materialize -m poppusher --select "*"
```
