# tests/test_config.py
import pytest

from sql_ai.config import Config


@pytest.fixture
def default_config(monkeypatch):
    monkeypatch.setenv("SQL_AI_FAKE_AWS_PROFILE", "playground")
    return Config()


@pytest.fixture
def custom_config():
    return Config(
        aws_account_id="123456789012",
        aws_profile="custom-profile",
        aws_region="us-east-1",
        aws_athena_s3_output_bucket="my-bucket",
        aws_athena_catalog="my-catalog",
        aws_athena_database="my-database",
        bedrock_model_key="claude-3.7",
        max_tokens=1000,
        temperature=0.5,
    )


def test_default_config(default_config):
    assert default_config.aws_account_id == "382901073838"
    assert default_config.aws_profile == "playground"
    assert default_config.aws_region == "eu-west-2"
    assert default_config.aws_athena_s3_output_bucket == ""
    assert default_config.aws_athena_catalog == "awsdatacatalog"
    assert default_config.aws_athena_database == "default"
    assert default_config.bedrock_model.name == "Claude 3.7"
    assert default_config.max_tokens == 2000
    assert default_config.temperature == 0.9


def test_custom_config(custom_config):
    assert custom_config.aws_account_id == "123456789012"
    assert custom_config.aws_profile == "custom-profile"
    assert custom_config.aws_region == "us-east-1"
    assert custom_config.aws_athena_s3_output_bucket == "my-bucket"
    assert custom_config.aws_athena_catalog == "my-catalog"
    assert custom_config.aws_athena_database == "my-database"
    assert custom_config.bedrock_model.name == "Claude 3.7"
    assert custom_config.max_tokens == 1000
    assert custom_config.temperature == 0.5


@pytest.mark.parametrize(
    "aws_account_id, expected_profile",
    [
        ("123456789012", "resolved-profile"),
        ("987654321098", "another-profile"),
    ],
)
def test_aws_profile_resolution(mocker, aws_account_id, expected_profile):
    """
    Test the resolution of AWS profiles based on account ID.

    This test uses parameterization to check if the `aws_profile` is correctly
    resolved for different `aws_account_id` inputs using a mock of the
    `find_aws_profile_by_account_id` function.

    Args:
        mocker: A pytest mocker object used to mock the function call.
        aws_account_id: The AWS account ID to test with.
        expected_profile: The expected AWS profile to be resolved.
    """

    mocker.patch(
        "sql_ai.config.find_aws_profile_by_account_id", return_value=expected_profile
    )
    config = Config(aws_account_id=aws_account_id)
    assert config.aws_profile == expected_profile


def test_invalid_bedrock_model():
    """
    Test that an invalid Bedrock model key raises a ValueError.
    """
    with pytest.raises(ValueError):
        Config(aws_profile="test", bedrock_model_key="invalid-model")
