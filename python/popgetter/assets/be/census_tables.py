from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import requests
from dagster import (
    MetadataValue,
    asset,
)
from icecream import ic
from rdflib import Graph, URIRef
from rdflib.namespace import DCAT, DCTERMS, SKOS

from popgetter.metadata import (
    DataPublisher,
    MetricMetadata,
    SourceDataRelease,
)

from .belgium import asset_prefix, country

publisher: DataPublisher = DataPublisher(
    name="Statbel",
    url="https://statbel.fgov.be/en",
    description="Statbel is the Belgian statistical office. It is part of the Federal Public Service Economy, SMEs, Self-employed and Energy.",
    countries_of_interest=[country],
)

source: SourceDataRelease = SourceDataRelease(
    name="StatBel Open Data",
    date_published=date(2015, 10, 22),
    reference_period=(date(2015, 10, 22), None),
    collection_period=(date(2015, 10, 22), None),
    expect_next_update=date(2022, 1, 1),
    url="https://statbel.fgov.be/en/open-data",
    publishing_organisation=publisher,
    description="TBC",
    geography_file="TBC",
    geography_level="Municipality",
    # available_metrics=None,
    countries_of_interest=[country],
)
source.update_forward_refs()


metrics: dict[str, MetricMetadata] = {
    "pop_per_muni": MetricMetadata(
        human_readable_name="Population by place of residence, nationality, marital status, age and sex",
        source_metric_id="pop_per_muni",
        description="Population by place of residence, nationality, marital status, age and sex, per municipality",
        hxl_tag="x_tbc",
        metric_parquet_file_url=None,
        parquet_column_name="MS_POPULATION",
        parquet_margin_of_error_column=None,
        parquet_margin_of_error_file=None,
        potential_denominator_ids=None,
        parent_metric_id=None,
        source_data_release_id=source.id,
        source_download_url="https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2023.zip",
        source_archive_file_path="TF_SOC_POP_STRUCT_2023.txt",
        source_documentation_url="https://statbel.fgov.be/en/open-data/population-place-residence-nationality-marital-status-age-and-sex-13",
    ),
    "pop_per_sector": MetricMetadata(
        human_readable_name="Population per statistical sector",
        source_metric_id="pop_per_sector",
        description="Population per statistical sector",
        hxl_tag="x_tbc",
        metric_parquet_file_url=None,
        parquet_column_name="MS_POPULATION",
        parquet_margin_of_error_column=None,
        parquet_margin_of_error_file=None,
        potential_denominator_ids=None,
        parent_metric_id=None,
        source_data_release_id=source.id,
        source_download_url="https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip",
        source_archive_file_path="OPENDATA_SECTOREN_2022.txt",
        source_documentation_url="https://statbel.fgov.be/en/open-data/population-statistical-sector-10",
    ),
}


@asset(key_prefix=asset_prefix)
def get_publisher_metadata():
    """
    Returns a DataPublisher of metadata about the publisher.
    """
    return publisher


@asset(key_prefix=asset_prefix)
def get_opendata_table_list(context) -> Graph:
    """
    Returns a list of all the tables available in the Statbel Open Data portal.
    """
    # URL of datafile
    url = "https://doc.statbel.be/publications/DCAT/DCAT_opendata_datasets.ttl"

    graph = Graph()
    graph.parse(url, format="ttl")

    context.add_output_metadata(
        metadata={
            "num_records": len(graph),
        }
    )

    return graph


def filter_by_language(graph, predicate, language="en"):
    my_subject = URIRef("https://statbel.fgov.be/node/4689")

    values = []
    for first_round_value in graph.objects(
        subject=my_subject, predicate=predicate, unique=False
    ):
        if hasattr(first_round_value, "language"):
            if first_round_value.language == language:
                values.append(first_round_value.value)
        else:
            values.append(first_round_value)

    ic(len(values))

    if len(values) == 1:
        return values.pop()

    # Handle DCAT.landingPage
    # For some reason the DCAT.landingPage doesn't seem to have a `language` attribute
    # but there are values for the four different languages.
    # THis _might_ be a bug in then BESTAT OpenData Catalogue, but for now we will manually
    # filter it here.
    if DCAT.landingPage.eq(predicate):
        # replicate the data structure we have above
        unfiltered_values = values
        values = []
        for first_round_value in unfiltered_values:
            my_subject2 = URIRef(first_round_value)
            for second_round_value in graph.objects(subject=my_subject2, unique=False):
                # Look up each of the four possible DCAT.landingPage
                # For each value there are four "full" top-level entries in the catalogue,
                # each with its own metadata. We search that metadata to determine the language

                # This is ridiculously over complicated, but seems to be the lease worst way to
                # filter by language without introducing a separate lookup.
                lang_graph = Graph()
                lang_graph.parse(second_round_value, format="xml")
                ic(second_round_value)
                for _, o in lang_graph.subject_objects(predicate=SKOS.notation):
                    ic(f"o={o}")
                    if hasattr(o, "datatype"):
                        ic(
                            f"o has `datatype` = {o.datatype}"  # pyright: ignore  # noqa: PGH003
                        )
                        iso2_type = URIRef(
                            "http://publications.europa.eu/ontology/euvoc#ISO_639_1"
                        )
                        if iso2_type.eq(o.datatype):  # pyright: ignore  # noqa: PGH003
                            ic("o is iso3_type")
                            ic(f"type(o)={type(o)}")
                            ic(
                                f"o.toPython()={o.toPython()}"  # pyright: ignore  # noqa: PGH003
                            )
                            ic(f"language.lower()={language.lower()}")
                            if (
                                language.lower()
                                == str(
                                    o.toPython()  # pyright: ignore  # noqa: PGH003
                                ).lower()
                            ):
                                ic("yeah! matched language!")
                                ic(
                                    f"adding first_round_value={first_round_value} to list of candidates"
                                )
                                values.append(first_round_value)

                # .URIRef('http://www.w3.org/2004/02/skos/core#notation')

                ic("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

            # if first_round_value.find(f"/{language}/") != -1:
            #     values.append(first_round_value)

    # Handle DCAT.distribution
    # Typically there are options (s, p, o triplets) for .xls and .zipped .txt files
    # We want the .zipped .txt files
    if DCAT.distribution.eq(predicate):
        # look up values and find the
        # dct:format <http://publications.europa.eu/resource/authority/file-type/TXT>
        # value
        # and then return the dcat:downloadURL value
        for v in values:
            ic(f"DCAT.distribution:v={v}")

    ic(len(values))

    if len(values) == 1:
        return values.pop()

    err_msg = "len(values)!=1"
    raise ValueError(err_msg)


@asset(key_prefix=asset_prefix)
def generate_metadata_from_table_list(
    context, get_opendata_table_list: Graph
) -> MetricMetadata:
    # for berevity
    graph = get_opendata_table_list

    mmd = MetricMetadata(
        human_readable_name=filter_by_language(graph, DCTERMS.title),
        source_metric_id="pop_per_sector",  # Defined in Popgetter
        description=filter_by_language(graph, DCTERMS.description),
        hxl_tag="x_tbc",  # Defined in Popgetter
        metric_parquet_file_url=None,
        parquet_column_name="MS_POPULATION",
        parquet_margin_of_error_column=None,
        parquet_margin_of_error_file=None,
        potential_denominator_ids=None,
        parent_metric_id=None,
        source_data_release_id=source.id,
        # source_download_url="https://statbel.fgov.be/sites/default/files/files/opendata/bevolking/sectoren/OPENDATA_SECTOREN_2022.zip",
        source_download_url=filter_by_language(graph, DCAT.distribution),
        source_archive_file_path="OPENDATA_SECTOREN_2022.txt",
        source_documentation_url=filter_by_language(graph, DCAT.landingPage),
    )

    ic(context)
    ic(mmd)

    return mmd


@asset(key_prefix=asset_prefix)
def get_population_details_per_municipality(context) -> pd.DataFrame:
    metadata = metrics["pop_per_muni"]
    return get_census_table(context, metadata)


@asset(key_prefix=asset_prefix)
def get_population_by_statistical_sector(context) -> pd.DataFrame:
    metadata = metrics["pop_per_sector"]
    return get_census_table(context, metadata)


def get_census_table(context, metric_metadata: MetricMetadata) -> pd.DataFrame:
    with TemporaryDirectory() as temp_dir:
        extracted_file = download_from_metric_metadata(metric_metadata, temp_dir)
        with Path(extracted_file).open() as f:
            population_df = pd.read_csv(f, sep="|", encoding="utf-8-sig")

    context.add_output_metadata(
        metadata={
            "num_records": len(population_df),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in population_df.columns.to_list()])
            ),
            "preview": MetadataValue.md(population_df.head().to_markdown()),
        }
    )

    return population_df


def download_from_metric_metadata(metadata: MetricMetadata, temp_dir) -> str:
    """
    Downloads a zip file from a URL and extracts it to a user-supplied temporary folder.

    It is expected that this will typically be used in a `with TemporaryDirectory:` statement, for example:

    ```
    with TemporaryDirectory() as temp_dir:
        extracted_file = download_zip(metadata, temp_dir)
        df = pd.read_csv(extracted_file)

    ```

    """

    return download_file(
        metadata.source_download_url, metadata.source_archive_file_path, temp_dir
    )


def download_file(source_download_url, source_archive_file_path, temp_dir) -> str:
    temp_dir = Path(temp_dir)
    is_zip: bool = source_archive_file_path is not None

    temp_file = temp_dir / "data.zip" if is_zip else temp_dir / "temp.file"

    with requests.get(source_download_url, stream=True) as r:
        r.raise_for_status()
        with Path(temp_file).open(mode="wb") as f:
            for chunk in r.iter_content(chunk_size=(16 * 1024 * 1024)):
                f.write(chunk)

    if is_zip:
        # We have a zip file, so extract the file we want from that
        with zipfile.ZipFile(temp_file, "r") as z:
            return z.extract(source_archive_file_path, path=temp_dir)
    else:
        return str(temp_file.resolve())
