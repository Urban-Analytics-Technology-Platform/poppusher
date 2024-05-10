from __future__ import annotations

import pandas as pd
from dagster import InputContext, MultiPartitionKey, OutputContext
from upath import UPath

from ..azure import ADLS2InnerIOManager
from . import TopLevelMetadataIOManager


class AzureTopLevelMetadataIOManager(TopLevelMetadataIOManager, ADLS2InnerIOManager):
    extension: str = ".parquet"

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        rel_path = self.get_relative_path(context)
        full_path = self.get_base_path() / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        context.add_output_metadata(metadata={"parquet_path": str(full_path)})
        self.dump_to_path(context, self.to_binary(obj), full_path)

    def _get_path_without_extension(
        self, context: InputContext | OutputContext
    ) -> UPath:
        if context.has_asset_key:
            context_path = self.get_asset_relative_path(context)
        else:
            # we are dealing with an op output
            context_path = self.get_op_output_relative_path(context)

        return self._base_path.joinpath(context_path)

    def _get_paths_for_partitions(
        self, context: InputContext | OutputContext
    ) -> dict[str, UPath]:
        """Returns a dict of partition_keys into I/O paths for a given context."""
        if not context.has_asset_partitions:
            raise TypeError(
                f"Detected {context.dagster_type.typing_type} input type "
                "but the asset is not partitioned"
            )

        def _formatted_multipartitioned_path(partition_key: MultiPartitionKey) -> str:
            ordered_dimension_keys = [
                key[1]
                for key in sorted(
                    partition_key.keys_by_dimension.items(), key=lambda x: x[0]
                )
            ]
            return "/".join(ordered_dimension_keys)

        formatted_partition_keys = {
            partition_key: (
                _formatted_multipartitioned_path(partition_key)
                if isinstance(partition_key, MultiPartitionKey)
                else partition_key
            )
            for partition_key in context.asset_partition_keys
        }

        asset_path = self._get_path_without_extension(context)
        return {
            partition_key: self._with_extension(
                self.get_path_for_partition(context, asset_path, partition)
            )
            for partition_key, partition in formatted_partition_keys.items()
        }
