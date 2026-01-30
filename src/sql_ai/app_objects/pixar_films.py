""" "defines objects for pixar films LLM"""

from typing import cast

from sql_ai.config import Config
from sql_ai.sql_backend.table import Table
from sql_ai.sql_backends.athena.athena_backend import AthenaBackend
from sql_ai.sql_backends.athena.prompt_defaults import (
    ATHENA_CONTEXT_TEMPLATE,
    ATHENA_GUIDELINES,
)
from sql_ai.sql_llm import SqlLLM
from sql_ai.sql_prompting.prompting import (
    SQLPrompt,
)

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


PixarConfig = Config(
    aws_account_id="688357424058",
    aws_region="eu-west-2",
    aws_athena_s3_output_bucket="athena-output-688357424058",
    aws_profile="personal",
    bedrock_model_key="claude-sonnet-3.7",
    max_tokens=2000,
    temperature=0.9,
    aws_athena_catalog=cast(str, pixar_films_table.catalog),
    aws_athena_database=pixar_films_table.database,
)

PixarBackend = AthenaBackend(
    output_bucket=PixarConfig.aws_athena_s3_output_bucket,
    tables=[pixar_films_table],
    database=PixarConfig.aws_athena_database,
    catalog=PixarConfig.aws_athena_catalog,
    aws_profile=PixarConfig.aws_profile,
    aws_region=PixarConfig.aws_region,
)

PixarLLM = SqlLLM(
    backend=PixarBackend,
    sql_prompt=PixarFilmsPrompt(),
    config=PixarConfig,
)
