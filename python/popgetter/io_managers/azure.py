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
    IOManager,
    OutputContext,
)
from dagster_azure.adls2.utils import ResourceNotFoundError, create_adls2_client
from dagster_azure.blob.utils import create_blob_client
from icecream import ic
from upath import UPath

from . import TopLevelGeometryIOManager, TopLevelMetadataIOManager

# Note: this might need to be longer for some large files, but there is an issue with header's not matching
_LEASE_DURATION = 60  # One minute

# Set connection timeout to be larger than default:
# https://github.com/Azure/azure-sdk-for-python/issues/26993#issuecomment-1289799860
_CONNECTION_TIMEOUT = 6000


class AzureIOManager(IOManager):
    storage_account: str | None = os.getenv("AZURE_STORAGE_ACCOUNT")
    container: str | None = os.getenv("AZURE_CONTAINER")
    prefix: str | None = os.getenv("AZURE_DIRECTORY")
    sas_token: str | None = os.getenv("SAS_TOKEN")
    adls2_client: DataLakeServiceClient
    file_system_client: FileSystemClient

    def __init__(self):
        if self.storage_account is None:
            err_msg = "Storage account needs to be provided."
            raise ValueError(err_msg)
        if self.sas_token is None:
            err_msg = "Credenital (SAS) needs to be provided."
            raise ValueError(err_msg)
        if self.container is None:
            err_msg = "Container needs to be provided."
            raise ValueError(err_msg)

        self.adls2_client = create_adls2_client(
            self.storage_account, AzureSasCredential(self.sas_token)
        )
        self.file_system_client = self.adls2_client.get_file_system_client(
            self.container
        )
        # Blob client needed to handle copying as ADLS doesn't have a copy API yet
        self.blob_client = create_blob_client(
            self.storage_account, AzureSasCredential(self.sas_token)
        )
        self.blob_container_client = self.blob_client.get_container_client(
            self.container
        )

        self.lease_duration = _LEASE_DURATION
        self.file_system_client.get_file_system_properties()

    def get_base_path(self) -> UPath:
        return UPath(self.prefix)

    @property
    def lease_client_constructor(self) -> Any:
        return DataLakeLeaseClient

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


class AzureTopLevelMetadataIOManager(TopLevelMetadataIOManager, AzureIOManager):
    def handle_output(self, context: OutputContext, df: pd.DataFrame) -> None:
        rel_path = self.get_relative_path(context)
        full_path = self.get_base_path() / rel_path
        context.add_output_metadata(metadata={"parquet_path": str(full_path)})
        self.dump_to_path(context, df.to_parquet(None), full_path)


class AzureGeoIOManager(TopLevelGeometryIOManager, AzureIOManager):
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
            err_msg = "pmtiles not currently implemented"
            raise ValueError(err_msg)
        else:
            value_error: str = f"'{output_type}' is not currently supported."
            raise ValueError(value_error)
        with Path(fname).open(mode="rb") as f:
            b: bytes = f.read()
            context.log.debug(ic(f"Size: {len(b) / (1_024 * 1_024):.3f}MB"))
            return b

    def handle_output(
        self,
        context: OutputContext,
        obj: tuple[pd.DataFrame, gpd.GeoDataFrame, pd.DataFrame],
    ) -> None:
        rel_paths = self.get_relative_paths(context, obj)
        base_path = self.get_base_path()
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
