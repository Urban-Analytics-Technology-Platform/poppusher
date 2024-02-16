from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path, PurePath
from tempfile import TemporaryDirectory

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from popgetter.utils import markdown_from_plot
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
from urllib.parse import urlparse

from .belgium import asset_prefix, country

publisher: DataPublisher = DataPublisher(
    name="Statbel",
    url="https://statbel.fgov.be/en",
    description="Statbel is the Belgian statistical office. It is part of the Federal Public Service Economy, SMEs, Self-employed and Energy.",
    countries_of_interest=[country],
)

opendata_catalog_root = URIRef("http://data.gov.be/catalog/statbelopen")

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


@asset(key_prefix=asset_prefix)
def get_publisher_metadata():
    """
    Returns a DataPublisher of metadata about the publisher.
    """
    return publisher


@asset(key_prefix=asset_prefix)
def opendata_dataset_list(context) -> Graph:
    """
    Returns a list of all the tables available in the Statbel Open Data portal.
    """
    # URL of datafile
    catalog_url = "https://doc.statbel.be/publications/DCAT/DCAT_opendata_datasets.ttl"

    graph = Graph()
    graph.parse(catalog_url, format="ttl")

    dataset_nodes_ids = list(
        graph.objects(subject=opendata_catalog_root, predicate=DCAT.dataset, unique=False)
    )


    context.add_output_metadata(
        metadata={
            "graph_num_records": len(graph),
            "num_datasets": len(dataset_nodes_ids),
            "dataset_node_ids": "\n".join(iter([str(n) for n in dataset_nodes_ids])),
        }
    )

    return graph


# @asset(partitions_def=dataset_node_partition)
# def generate_metadata_from_dataset_list(
#     context, opendata_dataset_list: Graph
# ) -> pd.DataFrame:
#     subject_node = URIRef(context.asset_partition_key_for_output())

#     mmd = get_mmd_from_dataset_node(opendata_dataset_list, subject_node)
#     return get_census_table(context, mmd)


@asset(key_prefix=asset_prefix)
def catalog_as_dataframe(context, opendata_dataset_list: Graph) -> pd.DataFrame:

    # Create the schema for the catalog
    catalog_summary = {
        "node": [],
        "human_readable_name": [],        # =filter_by_language(graph, dataset_node, DCTERMS.title),
        # "source_metric_id": [],        # ="pop_per_sector",  # Defined in Popgetter
        "description": [],        # =filter_by_language(graph, dataset_node, DCTERMS.description),
        # "hxl_tag": [],        # ="x_tbc",  # Defined in Popgetter
        "metric_parquet_file_url": [],        # =None,
        "parquet_column_name": [],        # ="MS_POPULATION",
        "parquet_margin_of_error_column": [],        # =None,
        "parquet_margin_of_error_file": [],        # =None,
        "potential_denominator_ids": [],        # =None,
        "parent_metric_id": [],        # =None,
        "source_data_release_id": [],        # =source.id,
        "source_download_url": [],        # =filter_by_language(graph, dataset_node, DCAT.distribution),
        "source_format": [],        # =filter_by_language(graph, dataset_node, DCTERMS.format),
        "source_archive_file_path": [],        # ="OPENDATA_SECTOREN_2022.txt",
        "source_documentation_url": [],        # =filter_by_language(

    }

    # Loop over the datasets in the catalogue Graph
    catalog_root = URIRef("http://data.gov.be/catalog/statbelopen")
    for dataset_id in opendata_dataset_list.objects(
        subject=catalog_root, predicate=DCAT.dataset, unique=True
    ):
        catalog_summary["node"].append(str(dataset_id))
        catalog_summary["human_readable_name"].append(
            filter_by_language(
                graph=opendata_dataset_list, subject=dataset_id, predicate=DCTERMS.title
            )
        )
        catalog_summary["description"].append(
            filter_by_language(
                opendata_dataset_list, subject=dataset_id, predicate=DCTERMS.description
            )
        )

        catalog_summary["metric_parquet_file_url"].append(None)        # =None,
        catalog_summary["parquet_margin_of_error_column"].append(None)        # =None,
        catalog_summary["parquet_margin_of_error_file"].append(None)        # =None,
        catalog_summary["potential_denominator_ids"].append(None)        # =None,
        catalog_summary["parent_metric_id"].append(None)        # =None,
        catalog_summary["source_data_release_id"].append(source.id)        # =source.id,

        # Unknown at this stage
        catalog_summary["parquet_column_name"].append(None)        # ="MS_POPULATION",

        download_url, archive_file_path, format = get_distribution_url(opendata_dataset_list, dataset_id, DCAT.distribution)

        catalog_summary["source_download_url"].append(download_url)
        catalog_summary["source_archive_file_path"].append(archive_file_path)        # ="OPENDATA_SECTOREN_2022.txt",
        catalog_summary["source_format"].append(format)
        catalog_summary["source_documentation_url"].append(
            get_landpage_url(opendata_dataset_list, dataset_id, language="en")
        )      

    catalog_df = pd.DataFrame(data=catalog_summary)
    ic(catalog_df.head())
    ic(type(catalog_df))

    # Now create the dynamic partitions for later in the pipeline
    # First delete the old dynamic partitions from the previous run
    for partition in context.instance.get_dynamic_partitions("dataset_nodes"):
        context.instance.delete_dynamic_partition("dataset_nodes", partition)

    # Create a dynamic partition for the datasets listed in the catalogue
    context.instance.add_dynamic_partitions(
        "dataset_nodes",
        catalog_summary["node"],
    )

    # Now add some metadata to the context
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

    return catalog_df


def filter_by_language(graph, subject, predicate, language="en") -> str:

    ic(graph)
    ic(subject)
    ic(predicate)
    ic(language)

    language_lookup = {}
    for first_round_value in graph.objects(
        subject=subject, predicate=predicate, unique=True
    ):
        if hasattr(first_round_value, "language"):
            language_lookup[first_round_value.language] = first_round_value.value
        else:
            err_msg = (
                "no language attribute for result:\n"
                f"result={first_round_value}\n"
                f"subject={subject}\n"
                f"predicate={predicate}\n"
                "use `filter_by_language` only for language-tagged literals\n"
            )
            raise ValueError(err_msg)

    if language in language_lookup:
        return str(language_lookup[language])
    else:
        # hard code a preference order for the languages, if the requested language is not available
        for fall_back_lang in ["en", "fr", "de", "nl"]:
            if fall_back_lang in language_lookup:
                return str(language_lookup[fall_back_lang])

    # If we get here, we have no language match
    err_msg = (
        "error in filter_by_language\n"
        "len(values)!=1\n"
        "subject={subject}\n"
        "predicate={predicate}\n"
        "values={language_lookup}\n"
    ).format(subject=subject, predicate=predicate, values=language_lookup)
    ic(language_lookup)
    raise ValueError(err_msg)


def get_landpage_url(graph, subject, language="en") -> str:
    # Handle DCAT.landingPage
    # For some reason the DCAT.landingPage doesn't seem to have a `language` attribute
    # but there are values for the four different languages.
    # THis _might_ be a bug in then BESTAT OpenData Catalogue, but for now we will manually
    # filter it here.
    ic.disable()
    # replicate the data structure we have above
    unfiltered_values = list(graph.objects(
        subject=subject, predicate=DCAT.landingPage, unique=True
    ))
    values_by_language = {}
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

                        values_by_language[str(o.toPython())] = first_round_value  # pyright: ignore  # noqa: PGH003

                        # if (
                        #     language.lower()
                        #     == str(
                        #         o.toPython()  # pyright: ignore  # noqa: PGH003
                        #     ).lower()
                        # ):
                        #     ic("yeah! matched language!")
                        #     ic(
                        #         f"adding first_round_value={first_round_value} to list of candidates"
                        #     )
                        #     values.append(first_round_value)

            # .URIRef('http://www.w3.org/2004/02/skos/core#notation')

            ic("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        # if first_round_value.find(f"/{language}/") != -1:
        #     values.append(first_round_value)

    if language in values_by_language:
        return str(values_by_language[language])
    else:
        # hard code a preference order for the languages, if the requested language is not available
        for fall_back_lang in ["en", "fr", "de", "nl"]:
            if fall_back_lang in values_by_language:
                return str(values_by_language[fall_back_lang])


    # If we get here, we have no language match
    err_msg = (
        "error in get_landpage_url\n"
        "len(values)!=1\n"
        "subject={subject}\n"
        "predicate={predicate}\n"
        "values={values}\n"
    ).format(subject=subject, predicate=DCAT.landingPage, values=values_by_language)
    ic(values_by_language)
    raise ValueError(err_msg)



def get_distribution_url(graph, subject, language="en") -> tuple[str, str, str]:
    ic.disable()
    # Handle DCAT.distribution
    # Typically there are options (s, p, o triplets) for .xls and .zipped .txt files
    # We want the .zipped .txt files
    # look up values and find the
    # dct:format <http://publications.europa.eu/resource/authority/file-type/TXT>
    # value
    # and then return the dcat:downloadURL value

    # create lookup format:distribution_url
    format_lookup = {}

    for distribution_url_str in list(graph.objects(
        subject=subject, predicate=DCAT.distribution, unique=True
    )):
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
    # We rely on the fact that the insertion order of the keys `dict`` is preserved (python >=3.7)
    # (See https://stackoverflow.com/a/39980744)
    # The values are used to derive the archive_file_path name. This is a convention used in the BESTAT
    # OpenData catalogue, which varies with different filetypes. eg
    # foo.txt  ->  foo.zip
    # foo.geojson  ->  foo.geojson.zip
    # foo.sqlite  ->  foo.sqlite.zip
    preference_order = {
        "http://publications.europa.eu/resource/authority/file-type/TXT" : ".txt",
        "http://publications.europa.eu/resource/authority/file-type/GEOJSON" : "",
        "http://publications.europa.eu/resource/authority/file-type/BIN" : "",  # Actually sqlite
        "http://publications.europa.eu/resource/authority/file-type/CSV" : ".csv",
        "http://publications.europa.eu/resource/authority/file-type/GML" : ".gml",
        "http://publications.europa.eu/resource/authority/file-type/MDB" : ".mdb",
        "http://publications.europa.eu/resource/authority/file-type/SHP" : ".shp",
        "http://publications.europa.eu/resource/authority/file-type/XLSX" : ".xlsx",
    }

    for format_str in preference_order:
        format = URIRef(format_str)
        ic(format_str)
        ic(format)
        if format in format_lookup:
            ic(format_lookup[format])
            values = [format_lookup[format]]
            break

    ic(len(values))

    if len(values) != 1:
        err_msg = (
            "error in get_distribution_url\n"
            "len(values)!=1\n"
            "subject={subject}\n"
            "predicate={predicate}\n"
            "values={values}\n"
        ).format(subject=subject, predicate=DCAT.distribution, values=values)
        ic(values)
        raise ValueError(err_msg)

    url_str = str(values.pop())

    # Now we have the distribution_url, we need to find the filename
    # Although not stated in the BESTAT OpenData documentation, the filename is typically the last part of the url
    path = PurePath(urlparse(url_str).path)

    if path.suffix == ".zip":
        new_extention = preference_order[format_str]
        path = path.with_suffix(new_extention)

    return url_str, path.name, format_str



# def get_mmd_from_dataset_node(context, graph, dataset_node: URIRef) -> MetricMetadata:
#     return MetricMetadata(
#         human_readable_name=filter_by_language(graph, dataset_node, DCTERMS.title),
#         source_metric_id="pop_per_sector",  # Defined in Popgetter
#         description=filter_by_language(graph, dataset_node, DCTERMS.description),
#         hxl_tag="x_tbc",  # Defined in Popgetter
#         metric_parquet_file_url=None,
#         parquet_column_name="MS_POPULATION",
#         parquet_margin_of_error_column=None,
#         parquet_margin_of_error_file=None,
#         potential_denominator_ids=None,
#         parent_metric_id=None,
#         source_data_release_id=source.id,
#         source_download_url=get_distribution_url(graph, dataset_node, DCAT.distribution),
#         source_archive_file_path="OPENDATA_SECTOREN_2022.txt",
#         source_documentation_url=get_landpage_url(
#             graph, dataset_node, DCAT.landingPage
#         ),
#     )


@asset(partitions_def=dataset_node_partition, key_prefix=asset_prefix)
def get_census_table(context, catalog_as_dataframe) -> pd.DataFrame:

    handlers = {
        "http://publications.europa.eu/resource/authority/file-type/TXT": download_census_table,
        "http://publications.europa.eu/resource/authority/file-type/GEOJSON": download_census_geometry,
        "http://publications.europa.eu/resource/authority/file-type/BIN": no_op_format_handler,
        "http://publications.europa.eu/resource/authority/file-type/CSV": no_op_format_handler,
        "http://publications.europa.eu/resource/authority/file-type/GML": no_op_format_handler,
        "http://publications.europa.eu/resource/authority/file-type/MDB": no_op_format_handler,
        "http://publications.europa.eu/resource/authority/file-type/SHP": no_op_format_handler,
        "http://publications.europa.eu/resource/authority/file-type/XLSX": no_op_format_handler,
    }

    partition_key = context.asset_partition_key_for_output()
    ic(partition_key)
    row = ic(catalog_as_dataframe.loc[catalog_as_dataframe.node.isin([partition_key])])
    ic(row)

    format = row["source_format"].iloc[0]
    ic(format)

    handler = handlers.get(format, no_op_format_handler)
    return handler(context, row=row)


def no_op_format_handler(context, **kwargs):
    # return pd.DataFrame(kwargs["row"])
    raise ValueError("No implementation for this format yet.")


def download_census_table(context, **kwargs) -> pd.DataFrame:
    table_details = kwargs["row"]
    ic(table_details)
    source_download_url = table_details["source_download_url"].iloc[0]
    source_archive_file_path = table_details["source_archive_file_path"].iloc[0]
    ic(source_download_url)
    ic(source_archive_file_path)

    with TemporaryDirectory() as temp_dir:
        extracted_file = download_file(
            source_download_url, source_archive_file_path, temp_dir
        )

        udes = []
        with Path(extracted_file).open() as f:
            for encoding in ["utf-8", "utf-8-sig", "iso-8859-1", "windows-1252"]:
                try:
                    population_df = pd.read_csv(f, sep="|", encoding=encoding)
                    break
                except UnicodeDecodeError as ude:
                    udes.append(ude)

        if len(udes) > 0:
            raise udes[-1]

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


def download_census_geometry(context, **kwargs) -> gpd.GeoDataFrame:
    # def get_geometries(context: AssetExecutionContext) -> gpd.GeoDataFrame:
    """
    Downloads the Statistical Sector for Belgium and returns a GeoDataFrame.

    """
    table_details = kwargs["row"]
    # URL of datafile
    source_download_url = table_details["source_download_url"].iloc[0]
    source_archive_file_path = table_details["source_archive_file_path"].iloc[0]
    ic(source_download_url)
    ic(source_archive_file_path)

    with TemporaryDirectory() as temp_dir:
        extracted_file = download_file(
            source_download_url, source_archive_file_path, temp_dir
        )
        sectors_gdf = gpd.read_file(extracted_file)

    # Set the type of the index
    sectors_gdf.index = sectors_gdf.index.astype(str)

    # Plot and convert the image to Markdown to preview it within Dagster
    # Yes we do pass the `plt` object to the markdown_from_plot function and not the `ax` object
    # ax = sectors_gdf.plot(color="green")
    ax = sectors_gdf.plot(legend=False)
    ax.set_title(table_details["human_readable_name"].iloc[0])
    md_plot = markdown_from_plot(plt)

    context.add_output_metadata(
        metadata={
            "num_records": len(sectors_gdf),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in sectors_gdf.columns.to_list()])
            ),
            "preview": MetadataValue.md(
                sectors_gdf.loc[:, sectors_gdf.columns != "geometry"]
                .head()
                .to_markdown()
            ),
            "plot": MetadataValue.md(md_plot),
        }
    )

    return sectors_gdf


# def download_from_metric_metadata(metadata: MetricMetadata, temp_dir) -> str:
#     """
#     Downloads a zip file from a URL and extracts it to a user-supplied temporary folder.

#     It is expected that this will typically be used in a `with TemporaryDirectory:` statement, for example:

#     ```
#     with TemporaryDirectory() as temp_dir:
#         extracted_file = download_zip(metadata, temp_dir)
#         df = pd.read_csv(extracted_file)

#     ```

#     """

#     return download_file(
#         metadata.source_download_url, metadata.source_archive_file_path, temp_dir
#     )


def download_file(source_download_url, source_archive_file_path, temp_dir) -> str:
    temp_dir = Path(temp_dir)
    zip_expected: bool = source_archive_file_path is not None
    expected_extension = Path(source_archive_file_path).suffix

    temp_file = temp_dir / "data.zip" if zip_expected else temp_dir / "temp.file"

    with requests.get(source_download_url, stream=True) as r:
        r.raise_for_status()
        with Path(temp_file).open(mode="wb") as f:
            for chunk in r.iter_content(chunk_size=(16 * 1024 * 1024)):
                f.write(chunk)

    if zipfile.is_zipfile(temp_file):
        # We have a zip file, so extract the file we want from that
        with zipfile.ZipFile(temp_file, "r") as z:
            # if there is only one file in the zip, we can just extract it
            zip_contents = z.namelist()
            ic(zip_contents)

            # If there is only one file in the zip, we can just extract it, ignoring the specified file path
            if len(zip_contents) == 1:
                source_archive_file_path = zip_contents[0]
            elif sum([f.endswith(expected_extension) for f in zip_contents]) ==1:
                # If there are multiple files in the zip, but only one has the correct file extension, we can just extract it, ignoring the specified file path
                for f in zip_contents:
                    if f.endswith(expected_extension):
                        source_archive_file_path = f

            # Extract the file we want, assuming that we've found the identified the correct file
            if source_archive_file_path in zip_contents:           
                z.extract(source_archive_file_path, path=temp_dir)
                return str(temp_dir / source_archive_file_path)
            else:
                raise ValueError(
                    f"Could not find {source_archive_file_path} in the zip file {temp_file}"
                )

    else:
        return str(temp_file.resolve())
