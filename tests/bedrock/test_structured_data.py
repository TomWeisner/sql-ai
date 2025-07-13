import boto3

from sql_ai.athena.athena_llm import (
    AthenaLLM,
)
from sql_ai.athena.table import Table
from sql_ai.utils.utils import find_aws_profile_by_account_id


def instantiate_clients():
    session = boto3.Session(profile_name=find_aws_profile_by_account_id("382901073838"))
    bedrock_runtime_client = session.client("bedrock-runtime", region_name="eu-west-2")
    athena_client = session.client("athena", region_name="eu-west-2")
    return athena_client, bedrock_runtime_client


test_table_description = (
    "A lookup table for stations showing their names, abbrevations and NLC codes"
)

test_table = Table(
    name="station_lookup",
    description=test_table_description,
    catalog=None,
    database="default",
)


def instantiate_athena_llm():
    athena_client, bedrock_runtime_client = instantiate_clients()
    rag = AthenaLLM(
        tables=[test_table],
        athena_client=athena_client,
        bedrock_runtime_client=bedrock_runtime_client,
    )
    return rag
