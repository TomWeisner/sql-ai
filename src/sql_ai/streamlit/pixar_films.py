from sql_ai.athena.athena_llm import AthenaLLM
from sql_ai.athena.sql_prompting import (
    SQLPrompt,
)
from sql_ai.athena.table import Table
from sql_ai.streamlit.config_dataclass import Config

pixar_films_table = Table(
    name="films",
    description=("Films"),
    catalog="awsdatacatalog",
    database="pixar",
)

custom_guidelines = """
"""  # noqa: E501


class PixarFilmsPrompt(SQLPrompt):
    def __init__(self):
        super().__init__()

    def additional_guidelines(self):
        return custom_guidelines


PixarConfig = Config(
    aws_account_id="688357424058",
    aws_region="eu-west-2",
    aws_athena_output_bucket="athena-output-688357424058",
    aws_profile="default",
    aws_bedrock_model_id="anthropic.claude-3-sonnet-20240229-v1:0",
    aws_bedrock_model_version="bedrock-2023-05-31",
    max_tokens=2000,
    temperature=0.9,
)

PixarLLM = AthenaLLM(
    tables=[pixar_films_table],
    sql_prompt=PixarFilmsPrompt(),
    config=PixarConfig,
)
