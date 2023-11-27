from __future__ import annotations

from setuptools import find_packages, setup

setup(
    name="popgetter",
    packages=find_packages(exclude=["popgetter_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "geopandas",
        "docker",
        "lxml",
        "pyarrow",
        "fsspec",
        "aiohttp",
        "matplotlib",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
