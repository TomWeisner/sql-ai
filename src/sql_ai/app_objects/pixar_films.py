""" "defines objects for pixar films LLM"""

from typing import cast

from sql_ai.config import AthenaConfig, AwsConfig, BedrockConfig
from sql_ai.sql_backends.athena.athena_backend import AthenaBackend
from sql_ai.sql_backends.athena.prompt_defaults import (
    ATHENA_CONTEXT_TEMPLATE,
    ATHENA_GUIDELINES,
)
from sql_ai.sql_backends.table import Table
from sql_ai.sql_llm import SqlLLM
from sql_ai.sql_prompting.prompting import SQLPrompt

# data downloaded here: https://erictleung.com/pixarfilms/

# define table
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
        super().__init__(
            general_context_template=ATHENA_CONTEXT_TEMPLATE,
            general_guidelines=ATHENA_GUIDELINES,
        )

    def additional_guidelines(self):
        return custom_guidelines


PixarAwsConfig = AwsConfig(
    account_id="688357424058",
    region="eu-west-2",
    profile="personal",
)

PixarBedrockConfig = BedrockConfig(
    model_key="claude-sonnet-3.7",
    max_tokens=2000,
    temperature=0.9,
)

PixarAthenaConfig = AthenaConfig(
    output_bucket="athena-output-688357424058",
    catalog=cast(str, pixar_films_table.catalog),
    database=pixar_films_table.database,
)

PixarBackend = AthenaBackend(
    tables=[pixar_films_table],
    config=PixarAthenaConfig,
    aws_config=PixarAwsConfig,
)

PixarLLM = SqlLLM(
    backend=PixarBackend,
    sql_prompt=PixarFilmsPrompt(),
    aws_config=PixarAwsConfig,
    bedrock_config=PixarBedrockConfig,
)
