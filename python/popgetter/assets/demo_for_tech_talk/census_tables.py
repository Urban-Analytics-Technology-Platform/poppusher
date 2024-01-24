from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import requests
from dagster import (
    DynamicPartitionsDefinition,
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

from .census_geometry import demo_data_dir
from .demo_country import country

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

dataset_node_partition = DynamicPartitionsDefinition(name="dataset_nodes")


@asset()
def opendata_dataset_list(context) -> Graph:
    """
    Returns a list of all the tables available in the Statbel Open Data portal.
    """
    # DEMO: pretend we are downloading a file:
    catalog_file = demo_data_dir / "statbel_opendata_subset.ttl"
    catalog_root = URIRef("http://data.gov.be/catalog/statbelopen")

    graph = Graph()
    graph.parse(catalog_file, format="ttl")

    dataset_nodes_ids = list(
        graph.objects(subject=catalog_root, predicate=DCAT.dataset, unique=False)
    )

    # First delete the old dynamic partitions from the previous run
    for partition in context.instance.get_dynamic_partitions("dataset_nodes"):
        context.instance.delete_dynamic_partition("dataset_nodes", partition)

    # Create a dynamic partition for the datasets listed in the catalogue
    context.instance.add_dynamic_partitions(
        "dataset_nodes",
        dataset_nodes_ids,
    )

    context.add_output_metadata(
        metadata={
            "graph_num_records": len(graph),
            "num_datasets": len(dataset_nodes_ids),
            "dataset_node_ids": "\n".join(iter([str(n) for n in dataset_nodes_ids])),
        }
    )

    return graph


@asset(partitions_def=dataset_node_partition)
def generate_metadata_from_dataset_list(
    context, opendata_dataset_list: Graph
) -> pd.DataFrame:
    subject_node = URIRef(context.asset_partition_key_for_output())

    mmd = get_mmd_from_dataset_node(opendata_dataset_list, subject_node)
    return get_census_table(context, mmd)


@asset()
def catalog_as_dataframe(context, opendata_dataset_list: Graph) -> pd.DataFrame:
    catalog_summary = {
        "nodes": [],
        "title": [],
        "description": [],
    }

    catalog_root = URIRef("http://data.gov.be/catalog/statbelopen")
    for dataset_id in opendata_dataset_list.objects(
        subject=catalog_root, predicate=DCAT.dataset, unique=True
    ):
        catalog_summary["nodes"].append(dataset_id)
        catalog_summary["title"].append(
            filter_by_language(
                graph=opendata_dataset_list, subject=dataset_id, predicate=DCTERMS.title
            )
        )
        catalog_summary["description"].append(
            filter_by_language(
                opendata_dataset_list, subject=dataset_id, predicate=DCTERMS.description
            )
        )

    catalog_df = pd.DataFrame(data=catalog_summary)
    ic(catalog_df.head())
    ic(type(catalog_df))

    context.add_output_metadata(
        # Metadata can be any key-value pair
        metadata={
            "num_records": len(catalog_df),
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in catalog_df.columns.to_list()])
            ),
            "preview": MetadataValue.md(catalog_df.to_markdown()),
        }
    )

    return catalog_df.copy()


def filter_by_language(graph, subject, predicate, language="en"):
    # my_subject = URIRef("https://statbel.fgov.be/node/4689")

    values = []
    for first_round_value in graph.objects(
        subject=subject, predicate=predicate, unique=True
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
    ic.disable()
    if DCAT.landingPage.eq(predicate):
        # replicate the data structure we have above
        unfiltered_values = values
        values = []
        for first_round_value in unfiltered_values:
            my_subject2 = URIRef(first_round_value)
            for second_round_value in graph.objects(subject=my_subject2, unique=True):
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

    ic.enable()
    # Handle DCAT.distribution
    # Typically there are options (s, p, o triplets) for .xls and .zipped .txt files
    # We want the .zipped .txt files
    if DCAT.distribution.eq(predicate):
        # look up values and find the
        # dct:format <http://publications.europa.eu/resource/authority/file-type/TXT>
        # value
        # and then return the dcat:downloadURL value

        # create lookup format:distribution_url
        format_lookup = {}

        for distribution_url_str in values:
            ic(distribution_url_str)
            distribution_url = URIRef(distribution_url_str)

            for dist_info in graph.objects(
                subject=distribution_url, predicate=DCTERMS.format, unique=True
            ):
                ic(dist_info)
                format_lookup[dist_info] = distribution_url

        ic(format_lookup)

        # Now extract a distribution_url from the format_lookup, based on a preference order of formats:
        # Most preferred first
        preference_order = [
            "http://publications.europa.eu/resource/authority/file-type/TXT",
            "http://publications.europa.eu/resource/authority/file-type/GEOJSON",
            "http://publications.europa.eu/resource/authority/file-type/BIN",  # Actually sqlite
            "http://publications.europa.eu/resource/authority/file-type/CSV",
            "http://publications.europa.eu/resource/authority/file-type/GML",
            "http://publications.europa.eu/resource/authority/file-type/MDB",
            "http://publications.europa.eu/resource/authority/file-type/SHP",
            "http://publications.europa.eu/resource/authority/file-type/XLSX",
        ]

        for format_str in preference_order:
            format = URIRef(format_str)
            ic(format_str)
            ic(format)
            if format in format_lookup:
                ic(format_lookup[format])
                values = [format_lookup[format]]
                break

    ic(len(values))

    if len(values) == 1:
        return values.pop()

    err_msg = "len(values)!=1"
    raise ValueError(err_msg)


def get_mmd_from_dataset_node(graph, dataset_node: URIRef) -> MetricMetadata:
    return MetricMetadata(
        human_readable_name=filter_by_language(graph, dataset_node, DCTERMS.title),
        source_metric_id="pop_per_sector",  # Defined in Popgetter
        description=filter_by_language(graph, dataset_node, DCTERMS.description),
        hxl_tag="x_tbc",  # Defined in Popgetter
        metric_parquet_file_url=None,
        parquet_column_name="MS_POPULATION",
        parquet_margin_of_error_column=None,
        parquet_margin_of_error_file=None,
        potential_denominator_ids=None,
        parent_metric_id=None,
        source_data_release_id=source.id,
        source_download_url=filter_by_language(graph, dataset_node, DCAT.distribution),
        source_archive_file_path="OPENDATA_SECTOREN_2022.txt",
        source_documentation_url=filter_by_language(
            graph, dataset_node, DCAT.landingPage
        ),
    )


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
