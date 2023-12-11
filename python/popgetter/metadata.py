from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class CountryMetadata(BaseModel):
    name_en: str
    name_local: str
    iso3_code: str
    iso2_code: str


class DataPublisher(BaseModel):
    name: str
    url: str
    description: str
    countries_of_interest: list[CountryMetadata]


class SourceDataRelease(BaseModel):
    name: str
    date_published: date
    reference_period: tuple[
        date, date
    ]  # Range of time for which the data can be assumed to be valid
    collection_period: tuple[
        date, date
    ]  # Range of time for which the data was collected
    except_next_update: date
    url: str
    publishing_organisation: DataPublisher
    description: str
    geography_file: str
    geography_level: str
    available_metrics: list[MetricMetadata]


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
    metric_parquet_file_url: str = Field(
        description="The location (URL) of the parquet file that contains this metric value"
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
    source_data_release: SourceDataRelease
    # The extended metadata then contains any additional details about the metric which might be country specific.
