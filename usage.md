# Dagster based 

For countries which have been ported to Dagster, the download can be invoked via the Dagster CLI.



### Run a single job

Typically there is a single job per country. :

```bash
DAGSTER_HOME=$PWD/persist dagster job execute -m popgetter --job job_be
```

### Run a single asset

Within a country are like to be multiple assets. To materialize a single asset (and any required dependencies)

```bash
DAGSTER_HOME=$PWD/persist dagster asset materialize -m popgetter  --select be_municipalities_populations
```

If the asset name is not unique to the country, then.....


# Per country download
At present they is an individual way of invoking the relevant download for each country.

## England

1. Follow OS specific instructions to install `node`, `wget` and `python`
    - for MacOS:
```bash
brew install node
```
    - Linux / Windows TBC

2. Install dependencies
```bash
npm install -g mapshaper
pip install -r requirements-non-foss.txt
pip install -r requirements.txt
```

3. Run the download:
```bash
python england.py
```

## Northern Ireland

1. TBC: Install OS specific dependencies; `rust >= 1.72.0 (5680fa18f 2023-08-23)`
`rustup update`

2. Run the test which invokes the download:
```bash
cargo test test_northern_ireland
```

## Singapore
1. Install dependencies
```bash
pip install -r requirements.txt
```

2. Run the downloader
```bash
python singapore/population_by_age_gender.py
```


## Belgium

1. Install dependencies
```bash
pip install -r requirements.txt
```

2. Run the downloader
```bash
python belgium.py
```

## France

Add details...

## USA

Add details...

## Australia

Add details...





