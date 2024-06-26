from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from hashlib import sha256
from typing import Self

import jcs
import pandas as pd
from pydantic import BaseModel, Field, computed_field, model_validator


class MetadataBaseModel(BaseModel):
    def hash_class_vars(self):
        """
        Calculate a SHA256 hash from a class instance's variables. Used for
        generating unique and verifiable IDs for metadata classes.

        Note that `vars()` does not include properties, so the IDs themselves are
        not part of the hash, which avoids self-reference issues.
        """

        # Must copy the dict to avoid overriding the actual instance attributes!
        # Because we're only modifying dates -> strings, we don't need to perform a
        # deepcopy but all variables must be serializable
        def serializable_vars(obj: object) -> dict:
            variables = {}
            # Check if variables are serializable
            for key, val in vars(obj).items():
                try:
                    jcs.canonicalize(val)
                    variables[key] = val
                except Exception:
                    pass

            # Python doesn't serialise dates to JSON, have to convert to ISO 8601 first
            for key, val in variables.items():
                if isinstance(val, date):
                    variables[key] = val.isoformat()

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
    return cls.fix_types(pd.DataFrame([md.model_dump() for md in metadata_instances]))


class CountryMetadata(MetadataBaseModel):
    @computed_field
    @property
    def id(self) -> str:
        if self.iso3166_2 is not None:
            return self.iso3166_2.lower().replace("-", "_")
        return self.iso3.lower().replace("-", "_")

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

    @classmethod
    def fix_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.astype(
            {
                "iso3166_2": "string",
            }
        )


class DataPublisher(MetadataBaseModel):
    @computed_field
    @property
    def id(self) -> str:
        return self.hash_class_vars()

    name: str = Field(description="The name of the organisation publishing the data")
    url: str = Field(description="The url of the publisher's homepage.")
    description: str = Field(
        description="A brief description of the organisation publishing the data, including its mandate."
    )
    countries_of_interest: list[str] = Field(
        description="A list of country IDs for which the publisher has data available."
    )


class GeometryMetadata(MetadataBaseModel):
    @computed_field
    @property
    def id(self) -> str:
        return self.hash_class_vars()

    @computed_field
    @property
    def filename_stem(self) -> str:
        level = "_".join(self.level.lower().split())
        year = self.validity_period_start.year
        return f"{self.country_metadata.id}/geometries/{level}_{year}"

    country_metadata: CountryMetadata = Field(
        "The `CountryMetadata` associated with the geometry.", exclude=True
    )

    validity_period_start: date = Field(
        description="The start of the range of time for which the regions are valid (inclusive)"
    )
    validity_period_end: date = Field(
        description="The end of the range of time for which the regions are valid (inclusive). If the data is a single-day snapshot, this should be the same as `validity_period_start`."
    )
    level: str = Field(
        description="The geography level contained in the file (e.g. output area, LSOA, MSOA, etc)"
    )
    hxl_tag: str = Field(
        description="Humanitarian eXchange Language (HXL) description for the geography level"
    )


class SourceDataRelease(MetadataBaseModel):
    @computed_field
    @property
    def id(self) -> str:
        return self.hash_class_vars()

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
    geometry_metadata_id: str = Field(
        description="The ID of the geometry metadata associated with this data release"
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
    @computed_field
    @property
    def id(self) -> str:
        return self.hash_class_vars()

    human_readable_name: str = Field(
        description='A human readable name for the metric, something like "Total Population under 12 years old"'
    )
    source_metric_id: str = Field(
        description='The name of the metric that comes from the source dataset (for example in the ACS this might be "B001_E001" or something similar)'
    )
    description: str = Field(
        description="A longer description of the metric which might include info on the caveats for the metric"
    )
    hxl_tag: str = Field(
        description="Field description using the Humanitarian eXchange Language (HXL) standard"
    )
    metric_parquet_path: str = Field(
        description="The path to the parquet file that contains the metric"
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

    @classmethod
    def fix_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.astype(
            {
                "parquet_margin_of_error_column": "string",
                "parquet_margin_of_error_file": "string",
                "potential_denominator_ids": "object",
                "parent_metric_id": "string",
                "source_archive_file_path": "string",
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
