# tests/conftest.py
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sql_ai.athena.table import Table

# Add `src/` and `tests/` to sys.path if not already present
BASE_DIR = Path(__file__).resolve().parent.parent
for subdir in ["src", "tests"]:
    path = str(BASE_DIR / subdir)
    if path not in sys.path:
        sys.path.insert(0, path)


def pytest_runtest_setup(item):
    if "local_only" in item.keywords and os.getenv("CI") == "true":
        pytest.skip("Skipping local-only test in CI environment")


# ---------- helpers -------------------------------------------------
def _header(columns):
    """Convert a list of column labels into the shape returned by Athena."""
    return {"Data": [{"VarCharValue": c} for c in columns]}


def _data_row(n_cols, value="data"):
    """Return a single Athena row of identical values."""
    return {"Data": [{"VarCharValue": value} for _ in range(n_cols)]}


# ---------- fixture -------------------------------------------------


@pytest.fixture
def mock_athena_client():
    """
    A drop-in replacement for a real boto3 Athena client.

    It knows just enough of the API surface that fetch_athena_results()
    uses:  start_query_execution, get_query_execution, get_query_results.
    """
    cols = ["station_name", "three_alpha", "station_nlc", "station_name_cap"]
    n_data_rows = 10

    client = MagicMock(name="AthenaClient")

    # 1) start_query_execution
    client.start_query_execution.return_value = {"QueryExecutionId": "TEST_EXEC_ID"}

    # 2) get_query_execution  (always "SUCCEEDED")
    client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }

    # 3) get_query_results  (single page – no NextToken)
    client.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Label": c} for c in cols]},
            "Rows": [
                _header(cols),
                *[_data_row(len(cols), value="test") for _ in range(n_data_rows)],
            ],
        }
    }

    # No pagination in this simple mock
    client.get_query_results.return_value["NextToken"] = None

    return client


@pytest.fixture
def test_table():
    return Table(
        name="station_lookup",
        description="Test table",
        catalog="awsdatacatalog",
        database="default",
    )


@pytest.fixture
def mock_bedrock_client(test_table):
    mock_client = MagicMock()

    # Simulated model response (normally JSON returned from Bedrock)
    mock_response_body = MagicMock()
    catalog = test_table.catalog
    database = test_table.database
    table_name = test_table.name
    query = (
        f'SELECT AVG("price") '
        f'FROM \\"{catalog}\\".\\"{database}\\".\\"{table_name}\\" as \\"c\\"'
    )

    mock_response_body.read.return_value = (
        b"{\n" + f'  "outputText": "{query}"\n'.encode("utf-8") + b"}"
    )
    mock_client.invoke_model.return_value = {"body": mock_response_body}
    return mock_client


@pytest.fixture
def instantiate_clients(mock_athena_client, mock_bedrock_client):
    return mock_athena_client, mock_bedrock_client
