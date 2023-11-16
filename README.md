# popgetter

Popgetter is a convenience tool for downloading census data from a number of different jurisdictions and coercing the data into common formats. The aim is that city or region scale analysis can be easily [replicated](https://the-turing-way.netlify.app/reproducible-research/overview/overview-definitions.html#table-of-definitions-for-reproducibility) for different geographies, using the most detailed, locally available data. 

## What popgetter does and doesn't do

**Popgetter DOES:**

For each of the implemented countries:

- Download the most detailed geometries, for which census data is available.
- Download the most detailed census available for selected variables (currently focused on population and car ownership).
- Ensures that the geometries and census data join correctly.

  **Popgetter WILL**

  - present some standard metadata to allow the user to see which variables are available and any possible trade-off between geographic and demographic disaggregation.
  - publish the data in a set of common file types (eg CloudGeoBuff, Parquet, PMtiles).

**Popgetter DOES NOT:**

- It does not attempt to ensure that census variables are comparable between different jurisdictions. Nor does it attempt to ensure that the results of any analysis can be directly compared across multiple countries. 

## Getting started

At present, this is still a development project, so the first step is to clone the repo and then install using pip, with the `--editable` option:

```bash
git clone git@github.com:Urban-Analytics-Technology-Platform/popgetter.git
cd popgetter
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `popgetter/assets/` directory. New assets and jobs will need to be added to the `popgetter/__init__.py` file.

## Development

You can start writing assets in `popgetter/assets/` directory. New assets and jobs will need to be added to the `popgetter/__init__.py` file.

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `popgetter_tests` directory and you can run tests using `pytest`:

```bash
pytest popgetter_tests
```

### Repo structure

This is a [Dagster](https://dagster.io/) project, and the repo layout was created with the [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project) command.

There is code, which predates the migration to Dagster in the `previous_code` directory. In due course, this will be removed as the remaining countries are migrated to Dagster. There are usage instructions for this old code in `previous_code/previous_code_usage.md`.
