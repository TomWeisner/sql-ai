"""Additional guards around SqlLLM query execution (SELECT-only)."""

from unittest.mock import MagicMock

import pytest

from sql_ai.config import Config
from sql_ai.sql_llm import SqlLLM
from tests.conftest import DummyBackend


def _make_llm(backend: DummyBackend) -> SqlLLM:
    return SqlLLM(
        config=Config(aws_profile="test-profile"),
        backend=backend,
        session=MagicMock(),
        bedrock_runtime_client=MagicMock(),
    )


def test_run_query_rejects_non_select(dummy_backend: DummyBackend):
    llm = _make_llm(dummy_backend)
    query = "  UPDATE table SET x = 1"

    with pytest.raises(ValueError) as excinfo:
        llm.run_query(query)

    assert "Only SELECT statements are allowed" in str(excinfo.value)
    assert "UPDATE table SET x = 1" in str(excinfo.value)
    assert dummy_backend.run_called_with is None


def test_run_query_allows_select_with_whitespace(dummy_backend: DummyBackend):
    llm = _make_llm(dummy_backend)
    query = "   SELECT * FROM foo"

    df = llm.run_query(query)

    assert dummy_backend.run_called_with == "SELECT * FROM foo"
    assert not df.empty


def test_run_query_allows_with_cte(dummy_backend: DummyBackend):
    llm = _make_llm(dummy_backend)
    query = "WITH cte AS (SELECT 1) SELECT * FROM cte"

    df = llm.run_query(query)

    assert dummy_backend.run_called_with == query
    assert not df.empty


def test_run_query_strips_sql_prefix(dummy_backend: DummyBackend):
    llm = _make_llm(dummy_backend)
    query = "`sql WITH cte AS (SELECT 1) SELECT * FROM cte`"

    df = llm.run_query(query)

    assert dummy_backend.run_called_with == "WITH cte AS (SELECT 1) SELECT * FROM cte"
    assert not df.empty
