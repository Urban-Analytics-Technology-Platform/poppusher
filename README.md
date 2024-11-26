# poppusher

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]

[![PyPI version][pypi-version]][pypi-link]
[![Conda-Forge][conda-badge]][conda-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[![GitHub Issues][github-issues-badge]][github-issues-link]

<!-- SPHINX-START -->

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/Urban-Analytics-Technology-Platform/poppusher/workflows/CI/badge.svg
[actions-link]:             https://github.com/Urban-Analytics-Technology-Platform/poppusher/actions
[conda-badge]:              https://img.shields.io/conda/vn/conda-forge/poppusher
[conda-link]:               https://github.com/conda-forge/poppusher-feedstock
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/Urban-Analytics-Technology-Platform/poppusher/discussions
[github-issues-badge]: https://img.shields.io/static/v1?label=GitHub&message=Issues&color=blue&logo=github
[github-issues-link]:  https://github.com/Urban-Analytics-Technology-Platform/poppusher/issues
[pypi-link]:                https://pypi.org/project/poppusher/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/poppusher
[pypi-version]:             https://img.shields.io/pypi/v/poppusher
[rtd-badge]:                https://readthedocs.org/projects/poppusher/badge/?version=latest
[rtd-link]:                 https://poppusher.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->

Popgetter is a convenience tool for downloading census data from a number of
different jurisdictions and coercing the data into common formats. The aim is
that city or region scale analysis can be easily
[replicated](https://the-turing-way.netlify.app/reproducible-research/overview/overview-definitions.html#table-of-definitions-for-reproducibility)
for different geographies, using the most detailed, locally available data.

## What popgetter does and doesn't do

**Popgetter DOES:**

For each of the implemented countries:

- Download the most detailed geometries, for which census data is available.
- Download the most detailed census available for selected variables (currently
  focused on population and car ownership).
- Ensures that the geometries and census data join correctly.

**Popgetter WILL**

- present some standard metadata to allow the user to see which variables are
  available and any possible trade-off between geographic and demographic
  disaggregation.
- publish the data in a set of common file types (eg CloudGeoBuff, Parquet,
  PMtiles).

**Popgetter DOES NOT:**

- It does not attempt to ensure that census variables are comparable between
  different jurisdictions. Nor does it attempt to ensure that the results of any
  analysis can be directly compared across multiple countries.

## Getting started

At present, this is still a development project, so the first step is to clone
the repo and then install using pip, with the `--editable` option:

1. Create a virtual environment and activate it (you should be able to use your
   own choice of environment manager, such as `conda` or `venv`, but so far
   `pyenv` is the most tested with popgetter). eg:

```bash
python  -m venv poppusher_venv    # create a virtual environment called `poppusher_venv`
source poppusher_venv/bin/activate  # activate the virtual environment
```

2. Clone the repo and then do an 'editable' install:

```bash
git clone https://github.com/Urban-Analytics-Technology-Platform/poppusher.git
cd poppusher
pip install -e ".[dev]"
```

3. Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Development

You can start writing assets in `poppusher/assets/` directory. New assets and
jobs will need to be added to the `poppusher/__init__.py` file.

### Adding new Python dependencies

You can specify new Python dependencies in `pyproject.toml`.

### Unit testing

Tests are in the `poppusher_tests` directory and you can run tests using
`pytest`:

```bash
pytest
```

### Repo structure

This is a [Dagster](https://dagster.io/) project. The repo layout was initially
created with the
[`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project)
command. It has been subsequently updated using the
[copier](https://copier.readthedocs.io/en/stable/) command and the
[Scientific Python template](https://github.com/scientific-python/cookie).

There is code, which predates the migration to Dagster in the `previous_code`
directory. In due course, this will be removed as the remaining countries are
migrated to Dagster. There are usage instructions for this old code in
`previous_code/previous_code_usage.md`.
