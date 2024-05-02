from __future__ import annotations

from datetime import date
from hashlib import sha256
from typing import Self

import jcs
from pydantic import BaseModel, Field, computed_field, model_validator


def hash_class_vars(class_instance):
    """
    Calculate a SHA256 hash from a class instance's variables. Used for
    generating unique and verifiable IDs for metadata classes.

    Note that `vars()` does not include properties, so the IDs themselves are
    not part of the hash, which avoids self-reference issues.
    """
    variables = vars(class_instance)
    # Python doesn't serialise dates to JSON, have to convert to ISO 8601 first
    for key, val in variables.items():
        if isinstance(val, date):
            variables[key] = val.isoformat()
    return sha256(jcs.canonicalize(variables)).hexdigest()


class CountryMetadata(BaseModel):
    @computed_field
    @property
    def id(self) -> str:
        if self.iso3166_2 is not None:
            return self.iso3166_2.lower()
        return self.iso3.lower()

    name_short_en: str = Field(
        description="The short name of the country in English (for example 'Belgium')."
    )
    name_official: str = Field(
        description="The official name of the country (for example 'Kingdom of Belgium'). In English if available."
    )
    iso3: str = Field(
        description="The ISO 3166-1 alpha-3 code of the country (for example 'BEL')."
    )
    iso2: str = Field(
        description="The ISO 3166-1 alpha-2 code of the country (for example 'BE')."
    )
    iso3166_2: str | None = Field(
        description="If the territory is a 'principal subdivision', its ISO 3166-2 code (for example 'BE-VLG')."
    )


class DataPublisher(BaseModel):
    @computed_field
    @property
    def id(self) -> str:
        return hash_class_vars(self)

    name: str = Field(description="The name of the organisation publishing the data")
    url: str = Field(description="The url of the publisher's homepage.")
    description: str = Field(
        description="A brief description of the organisation publishing the data, including its mandate."
    )
    countries_of_interest: list[str] = Field(
        description="A list of country IDs for which the publisher has data available."
    )


class SourceDataRelease(BaseModel):
    @computed_field
    @property
    def id(self) -> str:
        return hash_class_vars(self)

    name: str = Field(
        description="The name of the data release, as given by the publisher"
    )
    date_published: date = Field(description="The date on which the data was published")
    reference_period_start: date = Field(
        description="The start of the range of time for which the data can be assumed to be valid (inclusive)"
    )
    reference_period_end: date = Field(
        description="The end of the range of time for which the data can be assumed to be valid (inclusive). If the data is a single-day snapshot, this should be the same as `reference_period_start`."
    )
    collection_period_start: date = Field(
        description="The start of the range of time during which the data was collected (inclusive)"
    )
    collection_period_end: date = Field(
        description="The end of the range of time during which the data was collected (inclusive). If the data were collected in a single day, this should be the same as `collection_period_start`."
    )
    expect_next_update: date = Field(
        description="The date on which is it expected that an updated edition of the data will be published. In some cases this will be the same as `reference_period_end`"
    )
    url: str = Field(description="The url of the data release.")
    data_publisher_id: str = Field(
        description="The ID of the publisher of the data release"
    )
    description: str = Field(description="A description of the data release")
    geography_file: str = Field(
        description="The path of the geography FlatGeobuf file, relative to the top level of the data release"
    )
    geography_level: str = Field(
        description="The geography level contained in the file (e.g. output area, LSOA, MSOA, etc)"
    )

    @model_validator(mode="after")
    def check_dates(self) -> Self:
        msg_template = "{s}_period_start must be before or equal to {s}_period_end"
        if self.reference_period_start > self.reference_period_end:
            error_msg = msg_template.format(s="reference")
            raise ValueError(error_msg)
        if self.collection_period_start > self.collection_period_end:
            error_msg = msg_template.format(s="collection")
            raise ValueError(error_msg)
        return self


class MetricMetadata(BaseModel):
    @computed_field
    @property
    def id(self) -> str:
        return hash_class_vars(self)

    human_readable_name: str = Field(
        description='A human readable name for the metric, something like "Total Population under 12 years old"'
    )
    source_metric_id: str = Field(
        description='The name of the metric that comes from the source dataset ( for example in the ACS this might be "B001_E001" or something similar'
    )
    description: str = Field(
        description="A longer description of the metric which might include info on the caveats for the metric"
    )
    hxl_tag: str = Field(
        description="Field description using the Humanitarian eXchange Language (HXL) standard"
    )
    metric_parquet_file_url: str | None = Field(
        description="The relative path output file that contains this metric value. This should be relative to the root of a base URL defined at project level and should NOT include the file extension"
    )
    parquet_column_name: str = Field(
        description="Name of column in the outputted parquet file which contains the metric"
    )
    parquet_margin_of_error_column: str | None = Field(
        description="Name of the column if any that contains the margin of error for the metric",
    )
    parquet_margin_of_error_file: str | None = Field(
        description="Location (url) of the parquet file that contains the margin of error for the metric",
    )
    potential_denominator_ids: list[str] | None = Field(
        description="A list of metrics which are suitable denominators for this metric."
    )
    parent_metric_id: str | None = Field(
        description="Metric if any which is the parent to this one ( some census data like the ACS is organised hierarchically, this can be useful for making the metadata more searchable)",
    )
    source_data_release_id: str = Field(
        description="The id of the data release from which this metric comes",
    )
    source_download_url: str = Field(
        description="The url used to download the data from source.",
    )
    source_archive_file_path: str | None = Field(
        description="(Optional), If the downloaded data is in an archive file (eg zip, tar, etc), this field is the path with the archive to locate the data file.",
    )
    source_documentation_url: str = Field(
        description="The documentation of the data release in human readable form.",
    )


EXPORTED_MODELS = [CountryMetadata, DataPublisher, SourceDataRelease, MetricMetadata]


def export_schema():
    """
    Generates a JSON schema for all the models in this script and outputs it to
    the specified directory, with the filename `popgetter_{VERSION}.json`.
    """
    import argparse
    import json
    from pathlib import Path

    from pydantic.json_schema import models_json_schema

    from popgetter import __version__

    parser = argparse.ArgumentParser(description=export_schema.__doc__)
    parser.add_argument(
        "out_dir", help="The directory to output the schema to. Must exist."
    )
    args = parser.parse_args()
    out_dir = Path(args.out_dir)

    _, top_level_schema = models_json_schema(
        [(model, "serialization") for model in EXPORTED_MODELS],
        title="popgetter_schema",
        description=f"Version {__version__}",
    )
    if not out_dir.exists():
        error_msg = f"Directory {out_dir} does not exist."
        raise FileNotFoundError(error_msg)
    with (out_dir / f"popgetter_{__version__}.json").open("w") as f:
        json.dump(top_level_schema, f, indent=2)
