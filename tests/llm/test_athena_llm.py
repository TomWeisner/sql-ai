from unittest.mock import patch, MagicMock

from sql_ai.athena.athena_llm import AthenaLLM
from sql_ai.athena.sql_prompting import (
    SQLPrompt,
)
from sql_ai.athena.table import Table
from sql_ai.streamlit.config_dataclass import Config


@patch("sql_ai.streamlit.config_dataclass.find_aws_profile_by_account_id")
@patch("boto3.Session")
def test_athena_llm_instantiation(mock_boto_session, mock_find_profile):
    mock_find_profile.return_value = "test_profile"

    # Mock athena client
    mock_athena_client = MagicMock()
    mock_athena_client.meta.service_model.service_name = "athena"

    # Mock bedrock client (could be more specific if needed)
    mock_bedrock_client = MagicMock()
    mock_bedrock_client.meta.service_model.service_name = "bedrock-runtime"

    # Configure .client() to return different mocks based on service name
    def client_side_effect(service_name, region_name=None):
        if service_name == "athena":
            return mock_athena_client
        elif service_name == "bedrock-runtime":
            return mock_bedrock_client
        else:
            raise ValueError(f"Unexpected service name: {service_name}")

    # Set up mock boto session
    mock_session = MagicMock()
    mock_session.client.side_effect = client_side_effect
    mock_boto_session.return_value = mock_session

    test_table = Table(
        name="station_lookup",
        description="test",
        catalog=None,
        database="default",
    )

    test_config = Config()
    assert test_config.aws_profile == "test_profile"

    llm = AthenaLLM(tables=[test_table], config=test_config)

    assert llm.tables[0].name == "station_lookup"
    assert llm.tables[0].catalog == "awsdatacatalog"
    assert len(llm.tables) == 1
    assert llm.max_tokens == 2000
    assert llm.athena_client.meta.service_model.service_name == "athena"
    assert llm.bedrock_runtime_client.meta.service_model.service_name == "bedrock-runtime"


def test_no_tables_supplied(mock_bedrock_client):
    """
    Tests that if no tables are supplied to generate_prompt_sql, it will only
    return a message indicating that no tables are found.
    """
    query, prompt, format_logs, error_trace = SQLPrompt().generate_sql(
        user_question="What is the average price of a car?",
        tables=[],
        bedrock_runtime_client=mock_bedrock_client,
    )
    assert query == "No tables found - unable to generate query."
    assert not prompt
    assert not format_logs


@patch("sql_ai.athena.sql_prompting.call_model_direct")
def test_one_table_supplied(mock_call_model_direct, mock_bedrock_client):
    """
    Tests that if one table is supplied to generate_prompt_sql, it will generate
    a query that includes the table name and relates to the users question.
    """
    mock_call_model_direct.return_value = """
        SELECT AVG("price")
        FROM "AwsDataCatalog"."default"."cars" as "c"
    """
    table = Table(
        name="cars",
        description="A table of cars",
        catalog="AwsDataCatalog",
        database="default",
    )
    sql_prompt = SQLPrompt()

    query, prompt, format_logs, error_trace = sql_prompt.generate_sql(
        user_question="What is the average price of a car?",
        tables=[table],
        bedrock_runtime_client=mock_bedrock_client,
    )
    assert 'FROM "AwsDataCatalog"."default"."cars" as "c"'.lower() in query.lower()
    assert '"price"'.lower() in query.lower()
    assert prompt
    assert not error_trace


@patch("sql_ai.athena.sql_prompting.call_model_direct")
def test_one_table_supplied_custom_schema(mock_call_model_direct, mock_bedrock_client):
    """
    Tests that if one table is supplied to generate_prompt_sql that has a
    custom schema defined, it will generate
    a query that utilises the schema supplied.
    """
    mock_call_model_direct.return_value = """
        SELECT AVG("cost")
        FROM "AwsDataCatalog"."default"."cars" as "c"
    """
    table = Table(
        name="cars",
        description="A table of cars and how much they cost",
        catalog="AwsDataCatalog",
        database="default",
        schema={"cost": "float", "model": "string"},
    )
    sql_prompt = SQLPrompt()

    query, prompt, format_logs, error_trace = sql_prompt.generate_sql(
        user_question="What is the average price of a car?",
        tables=[table],
        bedrock_runtime_client=mock_bedrock_client,
    )

    assert 'FROM "AwsDataCatalog"."default"."cars" as "c"'.lower() in query.lower()
    assert '"cost"'.lower() in query.lower()
    assert prompt
    assert not error_trace
