from __future__ import annotations

import pandas as pd
from dagster import ConfigurableIOManager, InputContext, OutputContext
from icecream import ic
from upath import UPath


class TopLevelMetadataIOManager(ConfigurableIOManager):
    # Mapping of asset keys to output filenames
    output_filenames: dict[str, str] = {
        "country_metadata": "country_metadata.parquet",
        "data_publisher": "data_publishers.parquet",
        "source_data_release": "source_data_releases.parquet",
        # New metadata struct, not yet defined
        # "geography_release": "geography_releases.parquet",
        # Figure out how to use this when the IOManager class is defined
        # "geometries": "geometries/{}",  # this.format(filename)
        # "metrics": "metrics/{}"  # this.format(filename)
    }

    def get_relative_path(self, context: OutputContext) -> UPath:
        try:
            ic(context.asset_key.path)
            path_components = list(context.asset_key.path)
            path_components[-1] = self.output_filenames[path_components[-1]]
            return UPath("/".join(path_components))
        except KeyError:
            err_msg = f"Only the asset keys {','.join(self.output_filenames.keys())} are compatible with this"
            raise ValueError(err_msg)

    def to_binary(self, obj: pd.DataFrame) -> bytes:
        return obj.to_parquet(None)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        raise RuntimeError("This IOManager is only for writing outputs")
