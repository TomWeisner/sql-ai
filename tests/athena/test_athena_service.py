# tests/test_athena_service.py
from unittest.mock import MagicMock

import pandas as pd
import pytest

from sql_ai.athena.athena_backend import AthenaBackend
from sql_ai.sql_backend.table import Table


@pytest.fixture
def mock_athena_client():
    return MagicMock(name="AthenaClient")


@pytest.fixture
def athena_backend(mock_athena_client):
    return AthenaBackend(
        output_bucket="test-bucket",
        client=mock_athena_client,
        tables=[
            Table(
                name="station_lookup",
                description="Test table",
                catalog="AwsDataCatalog",
                database="default",
            )
        ],
    )


def test_populate_schemas_single_table(athena_backend, mock_athena_client):
    # Returned by start_query_execution
    mock_athena_client.start_query_execution.return_value = {"QueryExecutionId": "qid"}

    # Polling loop should immediately exit
    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }

    # First (and only) page of results; include header + one DDL row, and no NextToken
    expected_data_str = "CREATE TABLE station_lookup (station_name string)"
    mock_athena_client.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Label": "ddl"}]},
            "Rows": [
                {"Data": [{"VarCharValue": "ddl"}]},  # header row
                {"Data": [{"VarCharValue": expected_data_str}]},
            ],
        }
    }

    athena_backend.tables[0].schema = None
    athena_backend.populate_schemas()
    assert athena_backend.tables[0].schema == {"station_name": "string"}


def test_get_schema_from_athena_nonexistent_table(athena_backend, mock_athena_client):
    # Mock the query execution response
    mock_athena_client.start_query_execution.return_value = {"QueryExecutionId": "qid"}
    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "FAILED"}}
    }

    # Get the schema
    table = Table(
        name="nonexistent_table",
        description="some description",
        catalog="AwsDataCatalog",
        database="default",
    )
    with pytest.raises(RuntimeError):
        athena_backend.get_schema_from_athena(table)


def test_run_query(athena_backend, mock_athena_client):
    # Mock the query execution response
    mock_athena_client.start_query_execution.return_value = {"QueryExecutionId": "qid"}
    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_athena_client.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": [{"Label": "column1"}]},
            "Rows": [
                {"Data": [{"VarCharValue": "value1"}]},
                {"Data": [{"VarCharValue": "value2"}]},
            ],
        }
    }

    # Run the query
    query = "SELECT * FROM station_lookup"
    result = athena_backend.run_query(query)

    # Assert the result
    assert isinstance(result, pd.DataFrame)
    assert result.shape == (2, 1)
    assert result.columns[0] == "column1"
    assert result.iloc[0, 0] == "value1"
    assert result.iloc[1, 0] == "value2"


def test_run_query_no_results(athena_backend, mock_athena_client):
    # Mock the query execution response
    mock_athena_client.start_query_execution.return_value = {"QueryExecutionId": "qid"}
    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_athena_client.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {"ColumnInfo": []},
            "Rows": [],
        }
    }

    # Run the query
    query = "SELECT * FROM station_lookup WHERE 1=0"
    result = athena_backend.run_query(query)

    # Assert the result
    assert isinstance(result, pd.DataFrame)
    assert result.empty
