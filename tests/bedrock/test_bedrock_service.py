import pandas as pd

from sql_ai.bedrock.bedrock_service import BedrockService


def test_data_to_prompt_empty_dataframe():
    df = pd.DataFrame()
    assert BedrockService.data_to_prompt(df) == "No data found."


def test_data_to_prompt_renames_single_col0_to_result():
    df = pd.DataFrame([[123]], columns=["_col0"])
    text = BedrockService.data_to_prompt(df)
    assert "result: 123" in text
    assert "_col0" not in text


def test_data_to_prompt_renames_multiple_col0_cols():
    df = pd.DataFrame([[1, 2]], columns=["_col0", "_col1"])
    text = BedrockService.data_to_prompt(df)
    assert "col1: 1" in text
    assert "col2: 2" in text
