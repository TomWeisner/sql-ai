"""AWS authentication helpers for the Streamlit app."""

from __future__ import annotations

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    ProfileNotFound,
    SSOTokenLoadError,
    TokenRetrievalError,
    UnauthorizedSSOTokenError,
)


def is_aws_auth_error(exc: BaseException) -> bool:
    auth_types = (
        NoCredentialsError,
        ProfileNotFound,
        SSOTokenLoadError,
        TokenRetrievalError,
        UnauthorizedSSOTokenError,
    )
    message_tokens = (
        "credential",
        "expired",
        "profile",
        "refresh failed",
        "sso",
        "token",
    )

    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, auth_types):
            return True
        if isinstance(current, ClientError):
            error_text = str(current).lower()
            if any(token in error_text for token in message_tokens):
                return True
        if isinstance(current, BotoCoreError):
            error_text = str(current).lower()
            if any(token in error_text for token in message_tokens):
                return True
        current = current.__cause__ or current.__context__

    error_text = str(exc).lower()
    return any(token in error_text for token in message_tokens)


def build_aws_login_message(profile_name: str, exc: BaseException | None = None) -> str:
    profile_name = profile_name.strip()
    if profile_name:
        command = f"aws sso login --profile {profile_name}"
        prefix = (
            f"AWS authentication failed for profile '{profile_name}'. "
            f"Try running `{command}` in a terminal, then retry in the app."
        )
    else:
        prefix = (
            "AWS authentication failed. Enter an AWS CLI profile name, run "
            "`aws sso login --profile <profile>` in a terminal, then retry in the app."
        )
    if exc is None:
        return prefix
    return f"{prefix}\n\nOriginal error: {exc}"
