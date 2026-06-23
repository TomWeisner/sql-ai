"""Tests for explicit config defaults and validation."""

import pytest

from sql_ai.config import AthenaConfig, AwsConfig, BedrockConfig, RedshiftConfig


def test_default_aws_config():
    config = AwsConfig()

    assert config.account_id == "382901073838"
    assert config.profile == ""
    assert config.region == "eu-west-2"


def test_bedrock_config_defaults():
    config = BedrockConfig()

    assert config.model_key == "claude-sonnet-4.6"
    assert config.model.name == "Claude Sonnet 4.6"
    assert config.inference_profile_id == ""
    assert config.max_tokens == 2000
    assert config.temperature == 0.9


def test_explicit_backend_configs_preserve_values():
    assert AthenaConfig(
        output_bucket="my-bucket",
        catalog="my-catalog",
        database="my-database",
    ) == AthenaConfig(
        output_bucket="my-bucket",
        catalog="my-catalog",
        database="my-database",
    )
    assert RedshiftConfig(
        cluster_identifier="cluster-1",
        workgroup_name="wg-1",
        database="analytics",
        db_user="reporting_user",
        secret_arn="arn:aws:secretsmanager:eu-west-2:123456789012:secret:demo",
    ) == RedshiftConfig(
        cluster_identifier="cluster-1",
        workgroup_name="wg-1",
        database="analytics",
        db_user="reporting_user",
        secret_arn="arn:aws:secretsmanager:eu-west-2:123456789012:secret:demo",
    )


def test_custom_bedrock_config_preserves_runtime_settings():
    config = BedrockConfig(
        model_key="claude-sonnet-4.5",
        max_tokens=1000,
        temperature=0.5,
    )

    assert config.model.name == "Claude Sonnet 4.5"
    assert config.max_tokens == 1000
    assert config.temperature == 0.5


def test_bedrock_config_set_model_key_refreshes_model():
    config = BedrockConfig(model_key="claude-sonnet-4.6")

    config.set_model_key("claude-sonnet-3.0")

    assert config.model_key == "claude-sonnet-3.0"
    assert config.model.name == "Claude Sonnet 3"


def test_bedrock_config_set_inference_profile_id_refreshes_model():
    config = BedrockConfig(model_key="claude-sonnet-4.6")

    config.set_inference_profile_id("profile-arn")

    assert config.inference_profile_id == "profile-arn"
    assert config.model.invoke_model_id == "profile-arn"


def test_aws_config_preserves_explicit_profile():
    config = AwsConfig(account_id="123456789012", profile="explicit-profile")

    assert config.profile == "explicit-profile"


def test_invalid_bedrock_model():
    with pytest.raises(ValueError):
        BedrockConfig(model_key="invalid-model")
