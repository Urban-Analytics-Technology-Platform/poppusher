from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import geopandas as gpd
import pandas as pd
from azure.core.credentials import (
    AzureSasCredential,
)
from azure.storage.filedatalake import (
    DataLakeLeaseClient,
    DataLakeServiceClient,
    FileSystemClient,
)
from dagster import (
    Any,
    InputContext,
    MultiPartitionKey,
    OutputContext,
)
from dagster_azure.adls2.utils import ResourceNotFoundError
from icecream import ic
from upath import UPath

from ..azure import ADLS2InnerIOManager
from . import TopLevelMetadataIOManager
from .local import DagsterHomeMixin, TopLevelGeometryIOManager

# Note: this might need to be longer for some large files, but there is an issue with header's not matching
_LEASE_DURATION = 60  # One minute

# Set connection timeout to be larger than default:
# https://github.com/Azure/azure-sdk-for-python/issues/26993#issuecomment-1289799860
_CONNECTION_TIMEOUT = 6000


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
            error = (
                f"Detected {context.dagster_type.typing_type} input type "
                "but the asset is not partitioned"
            )
            raise TypeError(error)

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


class AzureGeoIOManager(TopLevelGeometryIOManager, DagsterHomeMixin):
    storage: str = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    container: str = os.getenv("AZURE_CONTAINER", "")
    prefix: str = os.getenv("AZURE_DIRECTORY", "")
    sas_token: str = os.getenv("SAS_TOKEN", "")
    adls2_client: DataLakeServiceClient
    file_system_client: FileSystemClient

    def __init__(self):
        def _create_url(storage_account, subdomain):
            return f"https://{storage_account}.{subdomain}.core.windows.net/"

        def create_adls2_client(
            storage_account: str, credential
        ) -> DataLakeServiceClient:
            """Create an ADLS2 client."""
            account_url = _create_url(storage_account, "dfs")
            return DataLakeServiceClient(account_url, credential)

        self.adls2_client = create_adls2_client(
            self.storage, AzureSasCredential(self.sas_token)
        )
        self.file_system_client = self.adls2_client.get_file_system_client(
            self.container
        )

        # # We also need a blob client to handle copying as ADLS doesn't have a copy API yet
        # self.blob_client = blob_client
        # self.blob_container_client = self.blob_client.get_container_client(file_system)

        self.lease_duration = _LEASE_DURATION
        self.file_system_client.get_file_system_properties()

    def get_base_path_local(self) -> UPath:
        return UPath(self.prefix)

    @property
    def lease_client_constructor(self) -> Any:
        return DataLakeLeaseClient

    @staticmethod
    def geo_df_to_bytes(context, geo_df: gpd.GeoDataFrame, output_type: str) -> bytes:
        tmp = tempfile.NamedTemporaryFile()
        if output_type.lower() == "parquet":
            fname = tmp.name + ".parquet"
            geo_df.to_parquet(fname)
        elif output_type.lower() == "flatgeobuf":
            fname = tmp.name + ".fgb"
            geo_df.to_file(fname, driver="FlatGeobuf")
        elif output_type.lower() == "geojsonseq":
            fname = tmp.name + ".geojsonseq"
            geo_df.to_file(fname, driver="GeoJSONSeq")
        elif output_type.lower() == "pmtiles":
            error = "pmtiles not currently implemented"
            raise ValueError(error)
        else:
            value_error: str = f"'{output_type}' is not currently supported."
            raise ValueError(value_error)
        with Path(fname).open(mode="rb") as f:
            b = f.read()
            context.log.debug(ic(f"Size: {len(b) / (1_024 * 1_024):.3f}MB"))
            return b

    def _uri_for_path(self, path: UPath, protocol: str = "abfss://") -> str:
        return "{protocol}{filesystem}@{account}.dfs.core.windows.net/{key}".format(
            protocol=protocol,
            filesystem=self.file_system_client.file_system_name,
            account=self.file_system_client.account_name,
            key=path.as_posix(),
        )

    def get_loading_input_log_message(self, path: UPath) -> str:
        return f"Loading ADLS2 object from: {self._uri_for_path(path)}"

    def get_writing_output_log_message(self, path: UPath) -> str:
        return f"Writing ADLS2 object at: {self._uri_for_path(path)}"

    def unlink(self, path: UPath) -> None:
        file_client = self.file_system_client.get_file_client(path.as_posix())
        with self._acquire_lease(file_client, is_rm=True) as lease:
            file_client.delete_file(lease=lease, recursive=True)

    def dump_to_path(self, context: OutputContext, obj: bytes, path: UPath) -> None:
        if self.path_exists(path):
            context.log.warning(f"Removing existing ADLS2 key: {path}")  # noqa: G004
            self.unlink(path)

        file = self.file_system_client.create_file(path.as_posix())
        with self._acquire_lease(file) as lease:
            # Note: chunk_size can also be specified, see API for Azure SDK for Python, DataLakeFileClient:
            # https://learn.microsoft.com/en-us/python/api/azure-storage-file-datalake/azure.storage.filedatalake.datalakefileclient
            file.upload_data(
                obj,
                lease=lease,
                overwrite=True,
                connection_timeout=_CONNECTION_TIMEOUT,
            )

    def path_exists(self, path: UPath) -> bool:
        try:
            self.file_system_client.get_file_client(
                path.as_posix()
            ).get_file_properties()
        except ResourceNotFoundError:
            return False
        return True

    @contextmanager
    def _acquire_lease(self, client: Any, is_rm: bool = False) -> Iterator[str]:
        lease_client = self.lease_client_constructor(client=client)
        try:
            lease_client.acquire(lease_duration=self.lease_duration)
            yield lease_client.id
        finally:
            # cannot release a lease on a file that no longer exists, so need to check
            if not is_rm:
                lease_client.release()

    def handle_output(
        self,
        context: OutputContext,
        obj: tuple[pd.DataFrame, gpd.GeoDataFrame, pd.DataFrame],
    ) -> None:
        rel_paths = self.get_relative_paths(context, obj)
        base_path = self.get_base_path_local()
        full_paths = {key: base_path / rel_path for key, rel_path in rel_paths.items()}
        for path in full_paths.values():
            path.parent.mkdir(parents=True, exist_ok=True)
        context.add_output_metadata(
            metadata={
                "geometry_metadata_path": str(full_paths["metadata"]),
                "flatgeobuf_path": str(full_paths["flatgeobuf"]),
                "pmtiles_path": str(full_paths["pmtiles"]),
                "geojsonseq_path": str(full_paths["geojsonseq"]),
                "names_path": str(full_paths["names"]),
            }
        )

        metadata_df, gdf, names_df = obj
        self.dump_to_path(context, metadata_df.to_parquet(None), full_paths["metadata"])
        self.dump_to_path(
            context,
            self.geo_df_to_bytes(context, gdf, "flatgeobuf"),
            full_paths["flatgeobuf"],
        )
        self.dump_to_path(
            context,
            self.geo_df_to_bytes(context, gdf, "geojsonseq"),
            full_paths["geojsonseq"],
        )
        # TODO: generate pmtiles
        self.dump_to_path(context, names_df.to_parquet(None), full_paths["names"])
