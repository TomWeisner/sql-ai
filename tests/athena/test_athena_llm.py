"""Tests for SqlLLM behavior specific to Athena setup/model selection."""

from unittest.mock import MagicMock

import pytest

from sql_ai.bedrock.models import MODEL_REGISTRY
from sql_ai.config import AwsConfig, BedrockConfig, ModelKey
from sql_ai.sql_llm import SqlLLM
from sql_ai.sql_prompting.prompting import SQLPrompt


def _make_aws_config() -> AwsConfig:
    return AwsConfig(
        profile="test-profile",
        account_id="123456789012",
        region="us-east-1",
    )


def _make_bedrock_config(
    model_key: ModelKey = "claude-sonnet-4.5",
) -> BedrockConfig:
    return BedrockConfig(model_key=model_key)


def test_sql_prompt_model_follows_config_choice():
    """Custom prompts should use the BedrockConfig-selected model."""
    aws_config = _make_aws_config()
    bedrock_config = _make_bedrock_config()
    custom_prompt = SQLPrompt("ctx {}", "guidelines")
    custom_prompt.model = MODEL_REGISTRY["claude-sonnet-3.0"]

    backend = MagicMock()
    backend.tables = []

    llm = SqlLLM(
        backend=backend,
        aws_config=aws_config,
        bedrock_config=bedrock_config,
        sql_prompt=custom_prompt,
        session=MagicMock(),
        bedrock_runtime_client=MagicMock(),
    )

    assert llm.sql_prompt.model is bedrock_config.model


def test_bedrock_call_receives_config_model():
    """BedrockService should be invoked with the BedrockConfig-selected model."""
    aws_config = _make_aws_config()
    bedrock_config = _make_bedrock_config()
    backend = MagicMock()
    backend.tables = []
    backend.format_query.return_value = MagicMock(
        formatted_sql="SELECT 1", logs=[], error_trace=""
    )

    llm = SqlLLM(
        backend=backend,
        aws_config=aws_config,
        bedrock_config=bedrock_config,
        session=MagicMock(),
        bedrock_runtime_client=MagicMock(),
    )

    llm.sql_prompt.build_prompt_body_for_sql = MagicMock(return_value={"messages": []})
    llm.bedrock.call = MagicMock(return_value="SELECT 1")

    llm.generate_sql(attempt_number=1, user_question="Question?")

    assert llm.bedrock.call.call_args.kwargs["model"] is bedrock_config.model


def test_sql_llm_requires_explicit_configs():
    backend = MagicMock()
    backend.tables = []

    with pytest.raises(
        ValueError, match="requires both `aws_config` and `bedrock_config`"
    ):
        SqlLLM(
            backend=backend,
            session=MagicMock(),
            bedrock_runtime_client=MagicMock(),
        )


def test_sql_llm_omits_blank_profile_when_building_session(monkeypatch):
    backend = MagicMock()
    backend.tables = []

    session_mock = MagicMock()
    session_mock.client.return_value = MagicMock()
    session_factory = MagicMock(return_value=session_mock)
    monkeypatch.setattr("sql_ai.sql_llm.boto3.Session", session_factory)

    SqlLLM(
        backend=backend,
        aws_config=AwsConfig(profile="", region="eu-west-2"),
        bedrock_config=BedrockConfig(),
    )

    session_factory.assert_called_once_with()
