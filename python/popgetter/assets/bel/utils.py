from pathlib import Path, PurePath
import requests
import sqlite3
from urllib.parse import urlparse
from icecream import ic
from dagster import MetadataValue
import csv
from rdflib import Graph, URIRef
from rdflib.namespace import DCAT, DCTERMS, SKOS
from tempfile import TemporaryDirectory
import pandas as pd
import zipfile
import geopandas as gpd


from popgetter.utils import extract_main_file_from_zip, markdown_from_plot

## Functions to process catalog

def filter_by_language(graph, subject, predicate, language="en") -> str:
    # build lookup of results by language
    language_lookup = {}
    for possible_results in graph.objects(
        subject=subject, predicate=predicate, unique=True
    ):
        if hasattr(possible_results, "language"):
            language_lookup[possible_results.language] = possible_results.value
        else:
            err_msg = (
                "no language attribute for result:\n"
                f"result={possible_results}\n"
                f"subject={subject}\n"
                f"predicate={predicate}\n"
                "use `filter_by_language` only for language-tagged literals\n"
            )
            raise ValueError(err_msg)

    # If we have the caller requested language, return that
    if language in language_lookup:
        return str(language_lookup[language])

    # hard code a preference order for the languages, if the requested language is not available
    for fall_back_lang in ["en", "fr", "de", "nl"]:
        if fall_back_lang in language_lookup:
            return str(language_lookup[fall_back_lang])

    # If we get here, we have not found a suitable language to match
    err_msg = (
        "error in filter_by_language\n"
        "len(values)!=1\n"
        f"subject={subject}\n"
        f"predicate={predicate}\n"
        f"values={language_lookup}\n"
    )
    ic(language_lookup)
    raise ValueError(err_msg)

def get_landpage_url(graph, subject, language="en") -> str:
    # Handle DCAT.landingPage
    # For some reason the DCAT.landingPage doesn't seem to have a `language` attribute
    # but there are values for the four different languages.
    # This _might_ be a bug in then BESTAT OpenData Catalogue, but for now we will manually
    # filter it here.
    ic.disable()
    # replicate the data structure we have above
    unfiltered_values = list(
        graph.objects(subject=subject, predicate=DCAT.landingPage, unique=True)
    )
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

                        values_by_language[
                            str(o.toPython())  # pyright: ignore  # noqa: PGH003
                        ] = first_round_value  # pyright: ignore  # noqa: PGH003

    # If we have the caller requested language, return that
    if language in values_by_language:
        return str(values_by_language[language])

    # hard code a preference order for the languages, if the requested language is not available
    for fall_back_lang in ["en", "fr", "de", "nl"]:
        if fall_back_lang in values_by_language:
            return str(values_by_language[fall_back_lang])

    # If we get here, we have no language match
    err_msg = (
        "error in get_landpage_url\n"
        "len(values)!=1\n"
        f"subject={subject}\n"
        f"predicate={DCAT.landingPage}\n"
        f"values={values_by_language}\n"
    )
    ic(values_by_language)
    raise ValueError(err_msg)

def get_distribution_url(graph, subject) -> tuple[str, str, str]:
    ic.disable()
    # Handle DCAT.distribution
    # Typically there are options (s, p, o triplets) for .xls and .zipped .txt files
    # We want the .zipped .txt files
    # look up values and find the
    # dct:format <http://publications.europa.eu/resource/authority/file-type/TXT>
    # value
    # and then return the dcat:downloadURL value

    # create lookup format:distribution_url
    format_lookup: dict[str, str] = {}

    for distribution_url_str in list(
        graph.objects(subject=subject, predicate=DCAT.distribution, unique=True)
    ):
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
        "http://publications.europa.eu/resource/authority/file-type/TXT": ".txt",
        "http://publications.europa.eu/resource/authority/file-type/GEOJSON": "",
        "http://publications.europa.eu/resource/authority/file-type/GML": ".gml",
        "http://publications.europa.eu/resource/authority/file-type/BIN": "",  # Actually sqlite
        "http://publications.europa.eu/resource/authority/file-type/CSV": ".csv",
        "http://publications.europa.eu/resource/authority/file-type/MDB": ".mdb",
        "http://publications.europa.eu/resource/authority/file-type/SHP": ".shp",
        "http://publications.europa.eu/resource/authority/file-type/XLSX": ".xlsx",
    }

    values: list[str] = []
    format_str: str = ""
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
            f"subject={subject}\n"
            f"predicate={DCAT.distribution}\n"
            f"values={values}\n"
        )
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


## Functions to download individual census tables

def no_op_format_handler(context, **kwargs):
    # Missing format handlers

    # sqlite compressed as tar.gz
    # https://statbel.fgov.be/node/595 # Census 2011 - Matrix of commutes by statistical sector

    # faulty zip file (confirmed by manual download)
    # https://statbel.fgov.be/en/node/2676

    # Excel only (French and Dutch only)
    # https://statbel.fgov.be/en/node/2654 # Geografische indelingen 2020
    # https://statbel.fgov.be/en/node/3961 # Geografische indelingen 2021

    # AccessDB only!
    # https://statbel.fgov.be/en/node/4135 # Enterprises subject to VAT according to legal form (English only)
    # https://statbel.fgov.be/en/node/4136 # Enterprises subject to VAT according to employer class (English only)

    err_msg = "No implementation for this format yet.\n" f"kwargs={kwargs}"
    context.log.error(err_msg)
    raise ValueError(err_msg)


def download_census_database(context, **kwargs) -> pd.DataFrame:
    table_details = kwargs["row"]
    ic(table_details)
    source_download_url = table_details["source_download_url"].iloc[0]
    source_archive_file_path = table_details["source_archive_file_path"].iloc[0]

    with TemporaryDirectory() as temp_dir:
        extracted_file = download_file(
            source_download_url, source_archive_file_path, temp_dir
        )

        conn = sqlite3.connect(extracted_file)
        sql_str = "SELECT name FROM sqlite_master WHERE type='table';"
        table_details_df = pd.read_sql_query(sql_str, conn)
        ic(table_details_df.head())

    context.add_output_metadata(
        metadata={
            "title": table_details["human_readable_name"].iloc[0],
            "num_tables": len(table_details_df),  # Metadata can be any key-value pair
            "table_details": MetadataValue.md(
                "\n".join(
                    [f"- '`{col}`'" for col in table_details_df.columns.to_list()]
                )
            ),
            "preview": MetadataValue.md(table_details_df.head().to_markdown()),
        }
    )

    return table_details_df


def download_census_table(context, **kwargs) -> pd.DataFrame:
    table_details = kwargs["row"]
    ic(table_details)
    source_download_url = table_details["source_download_url"].iloc[0]
    source_archive_file_path = table_details["source_archive_file_path"].iloc[0]
    ic(source_download_url)
    ic(source_archive_file_path)

    population_df = pd.DataFrame()
    with TemporaryDirectory() as temp_dir:
        extracted_file = download_file(
            source_download_url, source_archive_file_path, temp_dir
        )

        # This is probably overkill.
        # Whilst debugging I tried multiple encoding and error handling options
        # I'm leaving it in for now, but it could be simplified, if "utf-8" and "surrogateescape" are sufficient.
        udes: list[UnicodeDecodeError] = []
        for encoding in [
            "utf-8",
            "utf-8-sig",
            "ISO-8859-1",
            "windows-1252",
            "unicode_escape",
        ]:
            with Path(extracted_file).open(
                encoding=encoding, errors="surrogateescape"
            ) as f:
                try:
                    ic("trying encoding: ", encoding)
                    f.seek(0)
                    population_df = pd.read_csv(
                        f, sep="|", engine="python", quoting=csv.QUOTE_NONE
                    )
                    break
                except UnicodeDecodeError as ude:
                    udes.append(ude)

        if len(udes) > 0:
            err_msg = "Could not decode file {} with any of the following encodings:\n{}".format(
                extracted_file, "\n".join([str(e) for e in udes])
            )
            raise ValueError(err_msg)

    context.add_output_metadata(
        metadata={
            "title": table_details["human_readable_name"].iloc[0],
            "num_records": len(population_df),  # Metadata can be any key-value pair
            "columns": MetadataValue.md(
                "\n".join([f"- '`{col}`'" for col in population_df.columns.to_list()])
            ),
            "preview": MetadataValue.md(population_df.head().to_markdown()),
        }
    )

    return population_df


def download_census_geometry(context, **kwargs) -> gpd.GeoDataFrame:
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
    ax = sectors_gdf.plot(legend=False)
    ax.set_title(table_details["human_readable_name"].iloc[0])
    md_plot = markdown_from_plot()

    context.add_output_metadata(
        metadata={
            "title": table_details["human_readable_name"].iloc[0],
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
        return extract_main_file_from_zip(temp_file, temp_dir, expected_extension)

    return str(temp_file.resolve())


DOWNLOAD_HANDLERS = {
    "http://publications.europa.eu/resource/authority/file-type/TXT": download_census_table,
    "http://publications.europa.eu/resource/authority/file-type/GEOJSON": download_census_geometry,
    "http://publications.europa.eu/resource/authority/file-type/GML": download_census_geometry,
    "http://publications.europa.eu/resource/authority/file-type/BIN": download_census_database,
    "http://publications.europa.eu/resource/authority/file-type/CSV": no_op_format_handler,
    "http://publications.europa.eu/resource/authority/file-type/MDB": no_op_format_handler,
    "http://publications.europa.eu/resource/authority/file-type/SHP": no_op_format_handler,
    "http://publications.europa.eu/resource/authority/file-type/XLSX": no_op_format_handler,
}


## Functions to process tables

def nationality_to_string(n):
    if n == "ETR":
        return "non-Belgian"
    if n == "BEL":
        return "Belgian"
    raise ValueError

def married_status_to_string(cs):
    if cs == 1:
        return "single"
    if cs == 2:
        return "married"
    if cs == 3:
        return "widowed"
    if cs == 4:
        return "divorced"
    raise ValueError
