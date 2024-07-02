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
    IOManager,
    OutputContext,
)
from dagster_azure.adls2.utils import ResourceNotFoundError, create_adls2_client
from dagster_azure.blob.utils import create_blob_client
from icecream import ic
from upath import UPath

from . import (
    GeoIOManager,
    MetadataIOManager,
    MetricsIOManager,
    MetricsMetdataIOManager,
    MetricsSingleIOManager,
)

# Set no time limit on lease duration to enable large files to be uploaded
_LEASE_DURATION = -1

# Set connection timeout to be larger than default:
# https://github.com/Azure/azure-sdk-for-python/issues/26993#issuecomment-1289799860
_CONNECTION_TIMEOUT = 6000


class AzureMixin:
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

    def handle_df(self, context: OutputContext, df: pd.DataFrame, path: UPath) -> None:
        self.dump_to_path(context, df.to_parquet(None), path)


class AzureMetadataIOManager(AzureMixin, MetadataIOManager):
    pass


class AzureGeoIOManager(AzureMixin, GeoIOManager):
    def handle_flatgeobuf(
        self, context: OutputContext, geo_df: gpd.GeoDataFrame, full_path: UPath
    ) -> None:
        tmp = tempfile.NamedTemporaryFile()
        fname = tmp.name + ".fgb"
        geo_df.to_file(fname, driver="FlatGeobuf")
        with Path(fname).open(mode="rb") as f:
            b: bytes = f.read()
            context.log.debug(ic(f"Size: {len(b) / (1_024 * 1_024):.3f}MB"))
            self.dump_to_path(context, b, full_path)

    def handle_geojsonseq(
        self, context: OutputContext, geo_df: gpd.GeoDataFrame, full_path: UPath
    ) -> None:
        tmp = tempfile.NamedTemporaryFile()
        fname = tmp.name + ".geojsonseq"
        geo_df.to_file(fname, driver="GeoJSONSeq")
        with Path(fname).open(mode="rb") as f:
            b: bytes = f.read()
            context.log.debug(ic(f"Size: {len(b) / (1_024 * 1_024):.3f}MB"))
            self.dump_to_path(context, b, full_path)

    def handle_names(
        self, context: OutputContext, names_df: pd.DataFrame, full_path: UPath
    ) -> None:
        self.dump_to_path(context, names_df.to_parquet(None), full_path)

    def handle_geo_metadata(
        self, context: OutputContext, geo_metadata_df: pd.DataFrame, full_path: UPath
    ) -> None:
        self.dump_to_path(context, geo_metadata_df.to_parquet(None), full_path)


class AzureMetricsIOManager(AzureMixin, MetricsIOManager):
    pass


class AzureMetricsSingleIOManager(AzureMixin, MetricsSingleIOManager):
    pass


class AzureMetricsMetadataIOManager(AzureMixin, MetricsMetdataIOManager):
    pass


class AzureGeneralIOManager(AzureMixin, IOManager):
    """This class is used only for an asset which tests the Azure functionality
    (see cloud_outputs/azure_test.py). It is not used for publishing any
    popgetter data."""

    extension: str

    def __init__(self, extension: str | None = None):
        super().__init__()
        if extension is not None and not extension.startswith("."):
            err_msg = f"Provided extension ('{extension}') does not begin with '.'"
            raise ValueError(err_msg)
        self.extension = "" if extension is None else extension

    def handle_output(self, context: OutputContext, obj: bytes) -> None:
        path = self.get_base_path() / ".".join(
            [*context.asset_key.path, self.extension]
        )
        self.dump_to_path(context, obj, path)

    def load_input(self, context: InputContext) -> Any:
        return super().load_input(context)
