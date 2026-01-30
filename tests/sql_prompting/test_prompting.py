import pandas as pd

from sql_ai.bedrock.models import Model
from sql_ai.sql_prompting.prompting import SQLPrompt


def test_build_prompt_body_from_data_includes_question_and_context():
    prompt = SQLPrompt(general_context_template="Q: {}\n{}", general_guidelines="GUIDE")
    prompt.model = Model(name="Test", id="anthropic.claude-test", top_p=None)
    prompt.extra_context = "PREVIOUS CONTEXT"

    df = pd.DataFrame([[42]], columns=["value"])
    body = prompt.build_prompt_body_from_data("What is the answer?", df)
    message = body["messages"][0]["content"]

    assert "Most recent user question" in message
    assert "What is the answer?" in message
    assert "PREVIOUS CONTEXT" in message
    assert "SQL query results" in message
    assert "value: 42" in message
