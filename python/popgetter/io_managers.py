from dagster import ConfigurableIOManager, InputContext, OutputContext
import pandas as pd
from upath import UPath
import os


class ParquetIOManager(ConfigurableIOManager):
    """IO manager for (de)serialising dataframes to/from parquet files."""

    def get_base_path(self) -> UPath:
        dagster_home = os.getenv("DAGSTER_HOME")
        if not dagster_home:
            raise ValueError("The DAGSTER_HOME environment variable must be set.")
        return UPath(dagster_home) / "compliant_outputs"

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        try:
            rel_path = context.metadata["parquet_path"]
        except KeyError:
            raise ValueError(
                "To use ParquetIOManager, 'parquet_path' must be specified as part of the metadata on the @asset decorator."
            )
        full_path = self.get_base_path() / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        context.add_output_metadata(metadata={"parquet_path": str(full_path)})
        with full_path.open("wb") as file:
            obj.to_parquet(file)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        try:
            rel_path = context.upstream_output.metadata["parquet_path"]
        except (KeyError, AttributeError):
            raise ValueError("'parquet_path' was not specified on upstream asset.")
        full_path = self.get_base_path() / rel_path
        with full_path.open("rb") as file:
            return pd.read_parquet(file)
