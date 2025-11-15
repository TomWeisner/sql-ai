""" "defines objects for pixar films LLM"""

from typing import cast

from sql_ai.athena.athena_llm import AthenaLLM
from sql_ai.athena.sql_prompting.prompting import (
    SQLPrompt,
)
from sql_ai.athena.table import Table
from sql_ai.config import Config

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
    def additional_guidelines(self):
        return custom_guidelines


PixarConfig = Config(
    aws_account_id="688357424058",
    aws_region="eu-west-2",
    aws_athena_s3_output_bucket="athena-output-688357424058",
    aws_profile="personal",
    bedrock_model_key="claude-3.7",
    max_tokens=2000,
    temperature=0.9,
    aws_athena_catalog=cast(str, pixar_films_table.catalog),
    aws_athena_database=pixar_films_table.database,
)

PixarLLM = AthenaLLM(
    tables=[pixar_films_table],
    sql_prompt=PixarFilmsPrompt(),
    config=PixarConfig,
)
