"""Tests for SqlLLM query safety (only SELECT allowed)."""

from unittest.mock import MagicMock

import pytest

from sql_ai.config import Config
from sql_ai.sql_llm import SqlLLM
from tests.conftest import DummyBackend


def _make_llm(backend: DummyBackend) -> SqlLLM:
    config = Config(aws_profile="test-profile")
    return SqlLLM(
        config=config,
        backend=backend,
        session=MagicMock(),
        bedrock_runtime_client=MagicMock(),
    )


def test_run_query_blocks_destructive(dummy_backend: DummyBackend):
    llm = _make_llm(dummy_backend)

    with pytest.raises(ValueError):
        llm.run_query("DROP TABLE my_table")
    assert dummy_backend.run_called_with is None


def test_run_query_allows_select(dummy_backend: DummyBackend):
    llm = _make_llm(dummy_backend)

    df = llm.run_query("SELECT 1")

    assert dummy_backend.run_called_with == "SELECT 1"
    assert not df.empty
