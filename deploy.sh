#!/usr/bin/env bash
set -e

if [ -z "$ENV" ]; then
    echo "ENV environment variable not set; must be either 'dev' or 'prod'"
    exit 1
fi

if [ -z "$POPGETTER_COUNTRIES" ]; then
    echo "POPGETTER_COUNTRIES environment variable not set; must be comma-separated list of country IDs"
    exit 1
fi

export IGNORE_EXPERIMENTAL_WARNINGS=1
export DAGSTER_MODULE_NAME=popgetter
export DAGSTER_HOME=$(mktemp -d)
touch $DAGSTER_HOME/dagster.yaml  # Silences Dagster warnings

echo "Relevant environment variables:"
echo "  - POPGETTER_COUNTRIES: $POPGETTER_COUNTRIES"
echo "  - ENV: $ENV"
if [ $ENV == "prod" ]; then
    export AZURE_STORAGE_ACCOUNT=popgetter
    export AZURE_CONTAINER=prod
    export AZURE_DIRECTORY=$(python -c 'import popgetter; print(popgetter.__version__)' 2>/dev/null)
    if [ -z "$SAS_TOKEN" ]; then
        echo "SAS_TOKEN environment variable not set; it is required for Azure deployments"
        exit 1
    else
        echo "  - SAS_TOKEN: (exists)"
    fi
    echo "  - AZURE_STORAGE_ACCOUNT: $AZURE_STORAGE_ACCOUNT"
    echo "  - AZURE_CONTAINER: $AZURE_CONTAINER"
    echo "  - AZURE_DIRECTORY: $AZURE_DIRECTORY"
fi

echo "Generating popgetter data. This may take a while."
python -m popgetter.run all
