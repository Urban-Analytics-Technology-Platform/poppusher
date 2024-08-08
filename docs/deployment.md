# Deployment

This page explains how to generate and upload a complete set of metadata and
data, which conforms to the [output specification](output_structure.md), to
Azure blob storage.

## Overview

The deployment process is divided into two main steps:

1. **Fetching the data.** This means running the entire job for each country.
   This step does not publish anything to Azure; instead, it generates pickled
   data that is stored inside `$DAGSTER_HOME`.

2. **Uploading the data.** This means running each of the cloud sensor assets,
   which have the names `publish...`. There are four of these assets: one for
   the `countries.txt` file, one for the metadata structs, one for the
   geometries, and one for the metrics. These assets are tied to custom IO
   managers which read the pickled data from `$DAGSTER_HOME` and upload it to
   Azure blob storage.

Both of these steps are automated using the `popgetter.run` module, which is in
turn invoked by the `deploy.sh` script in the repository root.

## Required environment variables

The deployment process requires the following environment variables to be set:

- `$POPGETTER_COUNTRIES`: A comma-separated list of country IDs to generate data
  for. This list also feeds into the `countries.txt` file.

- `$ENV`: Set this to `prod` to deploy to Azure. (You can set it to `dev` too,
  but this will publish the data to a local temporary directory, so it's only
  useful for testing the script.)

- `$SAS_TOKEN`: The SAS token for the Azure blob storage account. Contact a
  popgetter maintainer if you need this.

So, a typical deployment command might look like this:

```bash
POPGETTER_COUNTRIES=bel,gb_nir ENV=prod SAS_TOKEN="..." ./deploy.sh
```

Note that the `SAS_TOKEN` value must be quoted, because it contains ampersands
which the shell will take to mean "run this command in the background".

## Where are the data stored?

The data and metadata will be uploaded to the
['popgetter' Azure storage account](https://portal.azure.com/#@turing.ac.uk/resource/subscriptions/06e7b12a-f395-4021-9fa2-5305fa01903e/resourceGroups/popgetter/providers/Microsoft.Storage/storageAccounts/popgetter/containersList)
under the Urban Analytics Technology Platform subscription: specifically, it
will be placed in the container named `prod`, and the directory corresponding to
the current version of popgetter.
