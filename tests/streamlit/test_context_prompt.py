from sql_ai.streamlit.context_prompt import build_previous_conversation_context


def test_build_previous_conversation_context_includes_completed_runs():
    runs = [
        {"status": "complete", "question": "Q1", "sql_query": "SQL1", "answer": "A1"},
        {"status": "complete", "question": "Q2", "answer": "A2"},
        {"status": "pending", "question": "Q3", "sql_query": "SQL3"},
        {"status": "complete", "question": "Q4"},
    ]

    context = build_previous_conversation_context("Latest", runs)

    assert "PREVIOUS CONVERSATION" in context
    assert "1. USER: Q1" in context
    assert "SQL: SQL1" in context
    assert "BOT: A1" in context
    assert "2. USER: Q2" in context
    assert "BOT: A2" in context
    assert "Q3" not in context
    assert "Q4" not in context
    assert "NEXT QUESTION TO ANSWER: Latest" in context


def test_build_previous_conversation_context_excludes_question_line():
    context = build_previous_conversation_context("Latest", [], include_question=False)
    assert "NEXT QUESTION TO ANSWER" not in context
