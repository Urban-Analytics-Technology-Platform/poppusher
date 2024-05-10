from __future__ import annotations

import os

import pandas as pd
from dagster import OutputContext
from upath import UPath

from . import TopLevelMetadataIOManager


class LocalTopLevelMetadataIOManager(TopLevelMetadataIOManager):
    dagster_home: str | None = os.getenv("DAGSTER_HOME")

    def get_base_path_local(self) -> UPath:
        if not self.dagster_home:
            raise ValueError("The DAGSTER_HOME environment variable must be set.")
        return UPath(self.dagster_home) / "cloud_outputs"

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        rel_path = self.get_relative_path(context)
        full_path = self.get_base_path() / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        context.add_output_metadata(metadata={"parquet_path": str(full_path)})
        with full_path.open("wb") as file:
            file.write(self.to_binary(obj))
