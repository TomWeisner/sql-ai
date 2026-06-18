from unittest.mock import MagicMock

from sql_ai.streamlit.handlers import process_question
from sql_ai.streamlit.query_controls import QueryControls


class DummyLLM:
    def __init__(self) -> None:
        self.tables = [object()]


def test_process_question_blocks_when_auth_notice_present(monkeypatch):
    fake_session_state = {"aws_auth_notice": "AWS login needed"}
    monkeypatch.setattr(
        "sql_ai.streamlit.handlers.st.session_state", fake_session_state, raising=False
    )
    error_mock = MagicMock()
    monkeypatch.setattr("sql_ai.streamlit.handlers.st.error", error_mock)

    result = process_question(
        DummyLLM(),
        "how many trains?",
        QueryControls(
            keep_context=True,
            use_supplied_sql=False,
            dry_run=True,
            interpret_with_llm=True,
        ),
    )

    assert result is False
    error_mock.assert_called_once_with("AWS login needed")
