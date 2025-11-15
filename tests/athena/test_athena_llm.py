from unittest.mock import MagicMock

from sql_ai.bedrock.models import MODEL_REGISTRY
from sql_ai.config import Config, ModelKey
from sql_ai.sql_llm import SqlLLM
from sql_ai.sql_prompting.prompting import SQLPrompt


def _make_config(model_key: ModelKey = "claude-4.5") -> Config:
    return Config(
        aws_profile="test-profile",
        aws_account_id="123456789012",
        aws_region="us-east-1",
        aws_athena_s3_output_bucket="bucket",
        aws_athena_catalog="catalog",
        aws_athena_database="database",
        bedrock_model_key=model_key,
    )


def test_sql_prompt_model_follows_config_choice():
    """Custom prompts should use the Config-selected Bedrock model."""
    config = _make_config()
    custom_prompt = SQLPrompt("ctx {}", "guidelines")
    custom_prompt.model = MODEL_REGISTRY["claude-3"]

    backend = MagicMock()
    backend.tables = []

    llm = SqlLLM(
        config=config,
        backend=backend,
        sql_prompt=custom_prompt,
        session=MagicMock(),
        bedrock_runtime_client=MagicMock(),
    )

    assert llm.sql_prompt.model is config.bedrock_model


def test_bedrock_call_receives_config_model():
    """BedrockService should be invoked with the Config-selected model."""
    config = _make_config()
    backend = MagicMock()
    backend.tables = []
    backend.format_query.return_value = MagicMock(
        formatted_sql="SELECT 1", logs=[], error_trace=""
    )

    llm = SqlLLM(
        config=config,
        backend=backend,
        session=MagicMock(),
        bedrock_runtime_client=MagicMock(),
    )

    llm.sql_prompt.build_prompt_body_for_sql = MagicMock(return_value={"messages": []})
    llm.bedrock.call = MagicMock(return_value="SELECT 1")

    llm.generate_sql(attempt_number=1, user_question="Question?")

    assert llm.bedrock.call.call_args.kwargs["model"] is config.bedrock_model
