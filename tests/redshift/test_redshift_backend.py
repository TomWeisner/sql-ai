"""Tests for RedshiftBackend schema population and query execution."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from sql_ai.sql_backend.table import Table
from sql_ai.sql_backends.redshift.redshift_backend import RedshiftBackend


@pytest.fixture
def mock_redshift_client():
    return MagicMock(name="RedshiftDataClient")


@pytest.fixture
def redshift_backend(mock_redshift_client):
    return RedshiftBackend(
        tables=[
            Table(
                name="station_lookup",
                description="Test table",
                catalog="_",
                database="public",
            )
        ],
        client=mock_redshift_client,
        database="dev",
        workgroup_name="wg",
        db_user="dbuser",
        secret_arn="arn:aws:secretsmanager:region:acct:secret:secret",
    )


def test_populate_schemas_single_table(redshift_backend, mock_redshift_client):
    mock_redshift_client.describe_table.return_value = {
        "ColumnList": [
            {"name": "station_name", "typeName": "varchar"},
            {"name": "station_id", "typeName": "int"},
        ]
    }

    redshift_backend.tables[0].schema = None
    redshift_backend.populate_schemas()

    assert redshift_backend.tables[0].schema["station_name"] == "varchar"
    mock_redshift_client.describe_table.assert_called_with(
        Database="dev",
        WorkgroupName="wg",
        DbUser="dbuser",
        SecretArn="arn:aws:secretsmanager:region:acct:secret:secret",
        Schema="public",
        Table="station_lookup",
    )


def test_run_query(redshift_backend, mock_redshift_client):
    mock_redshift_client.execute_statement.return_value = {"Id": "stmt-1"}
    mock_redshift_client.describe_statement.return_value = {"Status": "FINISHED"}
    mock_redshift_client.get_statement_result.return_value = {
        "ColumnMetadata": [{"label": "column1"}],
        "Records": [
            [{"stringValue": "value1"}],
            [{"stringValue": "value2"}],
        ],
    }

    result = redshift_backend.run_query("SELECT * FROM station_lookup")

    assert isinstance(result, pd.DataFrame)
    assert result.shape == (2, 1)
    assert list(result.columns) == ["column1"]
    assert result.iloc[0, 0] == "value1"
    assert result.iloc[1, 0] == "value2"


def test_run_query_failure_raises(redshift_backend, mock_redshift_client):
    mock_redshift_client.execute_statement.return_value = {"Id": "stmt-2"}
    mock_redshift_client.describe_statement.return_value = {
        "Status": "FAILED",
        "Error": "Syntax error",
    }

    with pytest.raises(RuntimeError) as excinfo:
        redshift_backend.run_query("SELECT * FROM broken")

    assert "Syntax error" in str(excinfo.value)
    assert "SELECT * FROM broken" in str(excinfo.value)


def test_run_query_no_results(redshift_backend, mock_redshift_client):
    mock_redshift_client.execute_statement.return_value = {"Id": "stmt-3"}
    mock_redshift_client.describe_statement.return_value = {"Status": "FINISHED"}
    mock_redshift_client.get_statement_result.return_value = {
        "ColumnMetadata": [],
        "Records": [],
    }

    result = redshift_backend.run_query("SELECT * FROM station_lookup WHERE 1=0")

    assert isinstance(result, pd.DataFrame)
    assert result.empty
