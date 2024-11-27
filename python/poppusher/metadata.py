from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from enum import Enum
from hashlib import sha256
from typing import Self

import jcs
import pandas as pd
from pydantic import BaseModel, Field, computed_field, model_validator


class COL(Enum):
    """
    This class stores the eventual column names of all the metadata classes,
    which are used when serialising the metadata to a dataframe.
    """

    GEO_ID = "GEO_ID"

    COUNTRY_ID = "country_id"
    COUNTRY_NAME_SHORT_EN = "country_name_short_en"
    COUNTRY_NAME_OFFICIAL = "country_name_official"
    COUNTRY_ISO3 = "country_iso3"
    COUNTRY_ISO2 = "country_iso2"
    COUNTRY_ISO3166_2 = "country_iso3166_2"

    DATA_PUBLISHER_ID = "data_publisher_id"
    DATA_PUBLISHER_NAME = "data_publisher_name"
    DATA_PUBLISHER_URL = "data_publisher_url"
    DATA_PUBLISHER_DESCRIPTION = "data_publisher_description"
    DATA_PUBLISHER_COUNTRIES_OF_INTEREST = "data_publisher_countries_of_interest"

    GEOMETRY_ID = "geometry_id"
    GEOMETRY_FILEPATH_STEM = "geometry_filepath_stem"
    GEOMETRY_VALIDITY_PERIOD_START = "geometry_validity_period_start"
    GEOMETRY_VALIDITY_PERIOD_END = "geometry_validity_period_end"
    GEOMETRY_LEVEL = "geometry_level"
    GEOMETRY_HXL_TAG = "geometry_hxl_tag"

    SOURCE_DATA_RELEASE_ID = "source_data_release_id"
    SOURCE_DATA_RELEASE_NAME = "source_data_release_name"
    SOURCE_DATA_RELEASE_DATE_PUBLISHED = "source_data_release_date_published"
    SOURCE_DATA_RELEASE_REFERENCE_PERIOD_START = (
        "source_data_release_reference_period_start"
    )
    SOURCE_DATA_RELEASE_REFERENCE_PERIOD_END = (
        "source_data_release_reference_period_end"
    )
    SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START = (
        "source_data_release_collection_period_start"
    )
    SOURCE_DATA_RELEASE_COLLECTION_PERIOD_END = (
        "source_data_release_collection_period_end"
    )
    SOURCE_DATA_RELEASE_EXPECT_NEXT_UPDATE = "source_data_release_expect_next_update"
    SOURCE_DATA_RELEASE_URL = "source_data_release_url"
    SOURCE_DATA_RELEASE_DATA_PUBLISHER_ID = "source_data_release_data_publisher_id"
    SOURCE_DATA_RELEASE_DESCRIPTION = "source_data_release_description"
    SOURCE_DATA_RELEASE_GEOMETRY_METADATA_ID = (
        "source_data_release_geometry_metadata_id"
    )

    METRIC_ID = "metric_id"
    METRIC_HUMAN_READABLE_NAME = "metric_human_readable_name"
    METRIC_SOURCE_METRIC_ID = "metric_source_id"
    METRIC_DESCRIPTION = "metric_description"
    METRIC_HXL_TAG = "metric_hxl_tag"
    METRIC_PARQUET_PATH = "metric_parquet_path"
    METRIC_PARQUET_COLUMN_NAME = "metric_parquet_column_name"
    METRIC_PARQUET_MARGIN_OF_ERROR_COLUMN = "metric_parquet_margin_of_error_column"
    METRIC_PARQUET_MARGIN_OF_ERROR_FILE = "metric_parquet_margin_of_error_file"
    METRIC_POTENTIAL_DENOMINATOR_IDS = "metric_potential_denominator_ids"
    METRIC_PARENT_METRIC_ID = "metric_parent_metric_id"
    METRIC_SOURCE_DATA_RELEASE_ID = "metric_source_data_release_id"
    METRIC_SOURCE_DOWNLOAD_URL = "metric_source_download_url"
    METRIC_SOURCE_ARCHIVE_FILE_PATH = "metric_source_archive_file_path"
    METRIC_SOURCE_DOCUMENTATION_URL = "metric_source_documentation_url"


class MetadataBaseModel(BaseModel):
    def hash_class_vars(self):
        """
        Calculate a SHA256 hash from a class instance's variables. Used for
        generating unique and verifiable IDs for metadata classes.

        Note that `vars()` does not include properties, so the IDs themselves are
        not part of the hash, which avoids self-reference issues.

        Furthermore, note that serialisation aliases are not used: the hash is
        calculated from the original field names.
        """

        # Must copy the dict to avoid overriding the actual instance attributes!
        # Because we're only modifying dates -> strings, we don't need to perform a
        # deepcopy but all variables must be serializable
        def serializable_vars(obj: object) -> dict:
            variables = {}
            # Check if variables are serializable
            for key, val in vars(obj).items():
                try:
                    # Python doesn't serialise dates to JSON, have to convert to ISO 8601 first
                    new_val = val.isoformat() if isinstance(val, date) else val
                    # Try to serialise
                    jcs.canonicalize(new_val)
                    # Store in dict if serialisable
                    variables[key] = new_val
                except Exception:
                    # If cannot serialise, continue
                    continue

            return variables

        return sha256(jcs.canonicalize(serializable_vars(self))).hexdigest()

    @classmethod
    def fix_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        When a list of MetadataBaseModel classes is converted to a dataframe,
        the types of the fields can be lost (notably, if all the values in a
        column are None). This method is implemented on each class in order to
        coerce the output dataframe columns to the correct types, so that the
        subsequent serialisation to parquet will retain this type information.

        In general, this function only needs to coerce columns which can be x
        or None to the type x itself --- for example, `iso3166_2` on the
        `CountryMetadata` class. For a column which is already a string (e.g.
        `iso3`), it should not be possible to instantiate the class with a None
        value, as Pydantic will raise an error.

        The default implementation of this method is to do nothing and just
        return the input dataframe. If a class needs to coerce types, it should
        override this method and return the modified dataframe.
        """
        return df


def metadata_to_dataframe(
    metadata_instances: Sequence[MetadataBaseModel],
):
    """
    Convert a list of metadata instances to a pandas DataFrame. Any of the five
    metadata classes defined in this module can be used here.
    """
    cls = metadata_instances[0].__class__
    return cls.fix_types(
        pd.DataFrame([md.model_dump(by_alias=True) for md in metadata_instances])
    )


class CountryMetadata(MetadataBaseModel):
    @computed_field(alias=COL.COUNTRY_ID.value)
    @property
    def id(self) -> str:
        if self.iso3166_2 is not None:
            return self.iso3166_2.lower().replace("-", "_")
        return self.iso3.lower().replace("-", "_")

    name_short_en: str = Field(
        description="The short name of the country in English (for example 'Belgium').",
        serialization_alias=COL.COUNTRY_NAME_SHORT_EN.value,
    )
    name_official: str = Field(
        description="The official name of the country (for example 'Kingdom of Belgium'). In English if available.",
        serialization_alias=COL.COUNTRY_NAME_OFFICIAL.value,
    )
    iso3: str = Field(
        description="The ISO 3166-1 alpha-3 code of the country (for example 'BEL').",
        serialization_alias=COL.COUNTRY_ISO3.value,
    )
    iso2: str = Field(
        description="The ISO 3166-1 alpha-2 code of the country (for example 'BE').",
        serialization_alias=COL.COUNTRY_ISO2.value,
    )
    iso3166_2: str | None = Field(
        description="If the territory is a 'principal subdivision', its ISO 3166-2 code (for example 'BE-VLG').",
        serialization_alias=COL.COUNTRY_ISO3166_2.value,
    )

    @classmethod
    def fix_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.astype(
            {
                COL.COUNTRY_ISO3166_2.value: "string",
            }
        )


class DataPublisher(MetadataBaseModel):
    @computed_field(alias=COL.DATA_PUBLISHER_ID.value)
    @property
    def id(self) -> str:
        return self.hash_class_vars()

    name: str = Field(
        description="The name of the organisation publishing the data",
        serialization_alias=COL.DATA_PUBLISHER_NAME.value,
    )
    url: str = Field(
        description="The URL of the publisher's homepage.",
        serialization_alias=COL.DATA_PUBLISHER_URL.value,
    )
    description: str = Field(
        description="A brief description of the organisation publishing the data, including its mandate.",
        serialization_alias=COL.DATA_PUBLISHER_DESCRIPTION.value,
    )
    countries_of_interest: list[str] = Field(
        description="A list of country IDs for which the publisher has data available.",
        serialization_alias=COL.DATA_PUBLISHER_COUNTRIES_OF_INTEREST.value,
    )


class GeometryMetadata(MetadataBaseModel):
    @computed_field(alias=COL.GEOMETRY_ID.value)
    @property
    def id(self) -> str:
        return self.hash_class_vars()

    @computed_field(alias=COL.GEOMETRY_FILEPATH_STEM.value)
    @property
    def filepath_stem(self) -> str:
        level = "_".join(self.level.lower().split())
        year = self.validity_period_start.year
        return f"{self.country_metadata.id}/geometries/{level}_{year}"

    country_metadata: CountryMetadata = Field(
        "The `CountryMetadata` associated with the geometry.", exclude=True
    )

    validity_period_start: date = Field(
        description="The start of the range of time for which the regions are valid (inclusive)",
        serialization_alias=COL.GEOMETRY_VALIDITY_PERIOD_START.value,
    )
    validity_period_end: date = Field(
        description="The end of the range of time for which the regions are valid (inclusive). If the data is a single-day snapshot, this should be the same as `validity_period_start`.",
        serialization_alias=COL.GEOMETRY_VALIDITY_PERIOD_END.value,
    )
    level: str = Field(
        description="The geography level contained in the file (e.g. output area, LSOA, MSOA, etc)",
        serialization_alias=COL.GEOMETRY_LEVEL.value,
    )
    hxl_tag: str = Field(
        description="Humanitarian eXchange Language (HXL) description for the geography level",
        serialization_alias=COL.GEOMETRY_HXL_TAG.value,
    )


class SourceDataRelease(MetadataBaseModel):
    @computed_field(alias=COL.SOURCE_DATA_RELEASE_ID.value)
    @property
    def id(self) -> str:
        return self.hash_class_vars()

    name: str = Field(
        description="The name of the data release, as given by the publisher",
        serialization_alias=COL.SOURCE_DATA_RELEASE_NAME.value,
    )
    date_published: date = Field(
        description="The date on which the data was published",
        serialization_alias=COL.SOURCE_DATA_RELEASE_DATE_PUBLISHED.value,
    )
    reference_period_start: date = Field(
        description="The start of the range of time for which the data can be assumed to be valid (inclusive)",
        serialization_alias=COL.SOURCE_DATA_RELEASE_REFERENCE_PERIOD_START.value,
    )
    reference_period_end: date = Field(
        description="The end of the range of time for which the data can be assumed to be valid (inclusive). If the data is a single-day snapshot, this should be the same as `reference_period_start`.",
        serialization_alias=COL.SOURCE_DATA_RELEASE_REFERENCE_PERIOD_END.value,
    )
    collection_period_start: date = Field(
        description="The start of the range of time during which the data was collected (inclusive)",
        serialization_alias=COL.SOURCE_DATA_RELEASE_COLLECTION_PERIOD_START.value,
    )
    collection_period_end: date = Field(
        description="The end of the range of time during which the data was collected (inclusive). If the data were collected in a single day, this should be the same as `collection_period_start`.",
        serialization_alias=COL.SOURCE_DATA_RELEASE_COLLECTION_PERIOD_END.value,
    )
    expect_next_update: date = Field(
        description="The date on which is it expected that an updated edition of the data will be published. In some cases this will be the same as `reference_period_end`",
        serialization_alias=COL.SOURCE_DATA_RELEASE_EXPECT_NEXT_UPDATE.value,
    )
    url: str = Field(
        description="The url of the data release.",
        serialization_alias=COL.SOURCE_DATA_RELEASE_URL.value,
    )
    data_publisher_id: str = Field(
        description="The ID of the publisher of the data release",
        serialization_alias=COL.SOURCE_DATA_RELEASE_DATA_PUBLISHER_ID.value,
    )
    description: str = Field(
        description="A description of the data release",
        serialization_alias=COL.SOURCE_DATA_RELEASE_DESCRIPTION.value,
    )
    geometry_metadata_id: str = Field(
        description="The ID of the geometry metadata associated with this data release",
        serialization_alias=COL.SOURCE_DATA_RELEASE_GEOMETRY_METADATA_ID.value,
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


class MetricMetadata(MetadataBaseModel):
    @computed_field(alias=COL.METRIC_ID.value)
    @property
    def id(self) -> str:
        return self.hash_class_vars()

    human_readable_name: str = Field(
        description='A human readable name for the metric, something like "Total Population under 12 years old"',
        serialization_alias=COL.METRIC_HUMAN_READABLE_NAME.value,
    )
    source_metric_id: str = Field(
        description='The name of the metric that comes from the source dataset (for example in the ACS this might be "B001_E001" or something similar)',
        serialization_alias=COL.METRIC_SOURCE_METRIC_ID.value,
    )
    description: str = Field(
        description="A longer description of the metric which might include info on the caveats for the metric",
        serialization_alias=COL.METRIC_DESCRIPTION.value,
    )
    hxl_tag: str = Field(
        description="Field description using the Humanitarian eXchange Language (HXL) standard",
        serialization_alias=COL.METRIC_HXL_TAG.value,
    )
    metric_parquet_path: str = Field(
        description="The path to the parquet file that contains the metric",
        serialization_alias=COL.METRIC_PARQUET_PATH.value,
    )
    parquet_column_name: str = Field(
        description="Name of column in the outputted parquet file which contains the metric",
        serialization_alias=COL.METRIC_PARQUET_COLUMN_NAME.value,
    )
    parquet_margin_of_error_column: str | None = Field(
        description="Name of the column if any that contains the margin of error for the metric",
        serialization_alias=COL.METRIC_PARQUET_MARGIN_OF_ERROR_COLUMN.value,
    )
    parquet_margin_of_error_file: str | None = Field(
        description="Location (url) of the parquet file that contains the margin of error for the metric",
        serialization_alias=COL.METRIC_PARQUET_MARGIN_OF_ERROR_FILE.value,
    )
    potential_denominator_ids: list[str] | None = Field(
        description="A list of metrics which are suitable denominators for this metric.",
        serialization_alias=COL.METRIC_POTENTIAL_DENOMINATOR_IDS.value,
    )
    parent_metric_id: str | None = Field(
        description="Metric if any which is the parent to this one ( some census data like the ACS is organised hierarchically, this can be useful for making the metadata more searchable)",
        serialization_alias=COL.METRIC_PARENT_METRIC_ID.value,
    )
    source_data_release_id: str = Field(
        description="The id of the data release from which this metric comes",
        serialization_alias=COL.METRIC_SOURCE_DATA_RELEASE_ID.value,
    )
    source_download_url: str = Field(
        description="The url used to download the data from source.",
        serialization_alias=COL.METRIC_SOURCE_DOWNLOAD_URL.value,
    )
    source_archive_file_path: str | None = Field(
        description="(Optional), If the downloaded data is in an archive file (eg zip, tar, etc), this field is the path with the archive to locate the data file.",
        serialization_alias=COL.METRIC_SOURCE_ARCHIVE_FILE_PATH.value,
    )
    source_documentation_url: str = Field(
        description="The documentation of the data release in human readable form.",
        serialization_alias=COL.METRIC_SOURCE_DOCUMENTATION_URL.value,
    )

    @classmethod
    def fix_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.astype(
            {
                COL.METRIC_PARQUET_MARGIN_OF_ERROR_COLUMN.value: "string",
                COL.METRIC_PARQUET_MARGIN_OF_ERROR_FILE.value: "string",
                COL.METRIC_POTENTIAL_DENOMINATOR_IDS.value: "object",
                COL.METRIC_PARENT_METRIC_ID.value: "string",
                COL.METRIC_SOURCE_ARCHIVE_FILE_PATH.value: "string",
            }
        )


EXPORTED_MODELS = [
    CountryMetadata,
    DataPublisher,
    SourceDataRelease,
    MetricMetadata,
    GeometryMetadata,
]


def export_schema():
    """
    Generates a JSON schema for all the models in this script and outputs it to
    the specified directory, with the filename `poppusher_{VERSION}.json`.
    """
    import argparse
    import json
    from pathlib import Path

    from pydantic.json_schema import models_json_schema

    from poppusher import __version__

    parser = argparse.ArgumentParser(description=export_schema.__doc__)
    parser.add_argument(
        "out_dir", help="The directory to output the schema to. Must exist."
    )
    args = parser.parse_args()
    out_dir = Path(args.out_dir)

    _, top_level_schema = models_json_schema(
        [(model, "serialization") for model in EXPORTED_MODELS],
        title="poppusher_schema",
        description=f"Version {__version__}",
    )
    if not out_dir.exists():
        error_msg = f"Directory {out_dir} does not exist."
        raise FileNotFoundError(error_msg)
    with (out_dir / f"poppusher_{__version__}.json").open("w") as f:
        json.dump(top_level_schema, f, indent=2)
