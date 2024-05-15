import pandas as pd
from dagster import asset


@asset(io_manager_key="azure_general_io_manager")
def test_azure():
    return pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}).to_parquet(None)


@asset(io_manager_key="azure_general_io_manager")
def test_azure_large():
    return b"0" * (450 * 1024 * 1024 + 100)
