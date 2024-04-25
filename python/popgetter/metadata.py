from __future__ import annotations

from datetime import date
from uuid import uuid4

from pydantic import BaseModel, Field


class CountryMetadata(BaseModel):
    name_short_en: str = Field(
        description="The short name of the country in English (for example 'Belgium')."
    )
    name_official: str = Field(
        description="The official name of the country (for example 'Kingdom of Belgium'). In English if available."
    )
    iso3: str = Field(description="The ISO3 code of the country (for example 'BEL').")
    iso2: str = Field(description="The ISO2 code of the country (for example 'BE').")
    iso3116_2: str | None = Field(
        description="The ISO3116-2 code for the names of the principal subdivisions (for example 'GB-SCT')."
    )


class DataPublisher(BaseModel):
    name: str = Field(description="The name of the organisation publishing the data")
    url: str = Field(description="The url of the publisher's homepage.")
    description: str = Field(
        description="A brief description of the organisation publishing the data, including its mandate."
    )
    countries_of_interest: list[CountryMetadata] = Field(
        description="A list of countries for which the publisher has data available."
    )


class SourceDataRelease(BaseModel):
    name: str = Field(
        description="The name of the data release, as given by the publisher"
    )
    id: str = Field(
        description="A unique identifier for the data release. This should be unique across all data releases from all publishers, but it is the client's responsibility to enforce this. If not specified, a UUID will be generated.",
        default_factory=lambda: str(uuid4()),
    )
    date_published: date = Field(description="The date on which the data was published")
    reference_period: tuple[date, date | None] = Field(
        description="The range of time for which the data can be assumed to be valid. Should be in the format (start_date, end_date)."
        " If the data represents a single day snapshot, end_date should be `None`."
    )
    collection_period: tuple[date, date | None] = Field(
        description="The range of time during which the data was collected. Should be in the format (start_date, end_date)."
        " If the data represents a single day snapshot, end_date should be `None`."
    )
    expect_next_update: date = Field(
        description="The date on which is it expected that an updated edition of the data will be published. In same cases this will be the same as the `reference_period[1]`."
    )
    url: str = Field(description="The url of the data release.")
    publishing_organisation: DataPublisher = Field(
        description="The publisher of the data"
    )
    description: str = Field(description="A description of the data release")
    geography_file: str = Field(description="The path of the geography file")
    geography_level: str = Field(description="The level of the geography")
    # available_metrics: list[MetricMetadata] | None = Field(
    #     description="A list of the available metrics"
    # )
    countries_of_interest: list[CountryMetadata] = Field(
        description="A list of the countries for which the data is available"
    )


class MetricMetadata(BaseModel):
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
        union_mode="smart",
        description="Name of the column if any that contains the margin of error for the metric",
    )
    parquet_margin_of_error_file: str | None = Field(
        union_mode="smart",
        description="Location (url) of the parquet file that contains the margin of error for the metric",
    )
    potential_denominator_ids: list[str] | None = Field(
        description="A list of metrics which are suitable denominators for this metric."
    )
    parent_metric_id: str | None = Field(
        union_mode="smart",
        description="Metric if any which is the parent to this one ( some census data like the ACS is organised hierarchically, this can be useful for making the metadata more searchable)",
    )
    source_data_release_id: str = Field(
        description="The id of the data release from which this metric comes",
    )
    source_download_url: str = Field(
        description="The url used to download the data from source.",
    )
    source_archive_file_path: str | None = Field(
        union_mode="smart",
        description="(Optional), If the downloaded data is in an archive file (eg zip, tar, etc), this field is the path with the archive to locate the data file.",
    )
    source_documentation_url: str = Field(
        description="The documentation of the data release in human readable form.",
    )


EXPORTED_MODELS = [CountryMetadata, DataPublisher, SourceDataRelease, MetricMetadata]


def export_schema():
    """
    Generates JSON schema for all the models in this script and prints them to
    stdout.
    """
    import json

    from pydantic.schema import schema

    top_level_schema = schema(EXPORTED_MODELS, title="popgetter_schema")
    print(json.dumps(top_level_schema, indent=2))  # noqa: T201
