from botocore.exceptions import SSOTokenLoadError, TokenRetrievalError

from sql_ai.streamlit.aws_auth import build_aws_login_message, is_aws_auth_error


def test_is_aws_auth_error_with_token_retrieval_error():
    exc = TokenRetrievalError(
        provider="sso", error_msg="Token has expired and refresh failed"
    )

    assert is_aws_auth_error(exc) is True


def test_is_aws_auth_error_with_nested_sso_error():
    try:
        try:
            raise SSOTokenLoadError(error_msg="expired")
        except SSOTokenLoadError as inner:
            raise RuntimeError("schema load failed") from inner
    except RuntimeError as exc:
        assert is_aws_auth_error(exc) is True


def test_build_aws_login_message_includes_profile_command():
    message = build_aws_login_message("playground")

    assert "playground" in message
    assert "aws sso login --profile playground" in message


def test_build_aws_login_message_without_profile():
    message = build_aws_login_message("")

    assert "Enter an AWS CLI profile name" in message
