from typing import Any, Optional

import boto3
import pandas as pd
import sqlglot

from sql_ai.athena.sql_prompting import (
    SQLPrompt,
)
from sql_ai.athena.table import Table
from sql_ai.athena.utils import get_schema_from_athena, run_query
from sql_ai.bedrock.utils import (
    call_model_direct,
    data_to_prompt,
    wrap_message_in_body,
)
from sql_ai.streamlit.utils import track_step_and_log
from sql_ai.utils.utils import find_aws_profile_by_account_id


class AthenaLLM:
    def __init__(
        self,
        tables: list[Table],
        athena_client: Any = None,
        bedrock_runtime_client: Any = None,
        athena_output_bucket: str = "",
        model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0",
        max_tokens: int = 2000,
        sql_prompt=None,
    ):
        self.athena_client = athena_client
        self.bedrock_runtime_client = bedrock_runtime_client
        self.tables = tables
        self.athena_output_bucket = athena_output_bucket
        self.model_id = model_id
        self.max_tokens = max_tokens
        self.max_sql_generation_retries = 3
        self.sql_prompt = sql_prompt

        if not self.athena_client:
            session = boto3.Session(
                profile_name=find_aws_profile_by_account_id("382901073838")
            )
            self.athena_client = session.client(
                "athena", region_name="eu-west-2"
            )  # type: ignore
        if not self.bedrock_runtime_client:
            session = boto3.Session(
                profile_name=find_aws_profile_by_account_id("382901073838")
            )
            self.bedrock_runtime_client = session.client(
                "bedrock-runtime-runtime", region_name="eu-west-2"
            )  # type: ignore
        assert (
            self.max_tokens > 0 and self.max_tokens <= 10000
        ), "max_tokens must be between 1 and 10000"
        if self.sql_prompt is None:
            self.sql_prompt = SQLPrompt(
                bedrock_runtime_client=self.bedrock_runtime_client
            )

    @track_step_and_log("sql_question")
    def sql_question(
        self, input: str, use_supplied_sql: bool = False
    ) -> tuple[str, dict, list[str], pd.DataFrame]:
        """For a given input, generate SQL and run it on Athena.
        Return the SQL, prompt, format logs and the results (as df)."""
        sql, prompt, format_logs, _ = self.get_sql(
            input=input, use_supplied_sql=use_supplied_sql
        )
        sql_results_df = self.run_athena_query(query=sql)
        return sql, prompt, format_logs, sql_results_df

    def get_sql(
        self, input: str, use_supplied_sql: bool = False
    ) -> tuple[str, dict, list[str], str]:
        if not use_supplied_sql:
            self._get_schemas_for_tables()
            sql, prompt, format_logs, error_traceback = self._generate_sql_with_retries(
                input, max_retries=self.max_sql_generation_retries
            )
        else:
            prompt = {}
            sql, format_logs, error_traceback = self.sql_prompt.formatter.format_sql(
                sql=input, tables=self.tables
            )
        return sql, prompt, format_logs, error_traceback

    def run_athena_query(self, query: str) -> pd.DataFrame:
        return run_query(query=query, client=self.athena_client)

    def dataframe_to_prompt(self, data: pd.DataFrame) -> str:
        return data_to_prompt(data=data)

    @track_step_and_log("✍️ Generating SQL")
    def _generate_sql_with_retries(
        self, input: str, max_retries: int = 3
    ) -> tuple[str, dict, list[str], str]:
        valid_sql_generation_retries = 0
        is_valid_sql: bool = False
        invalid_sql_prompt_additions = ""
        while valid_sql_generation_retries < max_retries and not is_valid_sql:
            valid_sql_generation_retries += 1
            sql, prompt, format_logs, error_traceback, is_valid_sql = self._generate_sql(
                attempt_number=valid_sql_generation_retries,
                user_question=input + invalid_sql_prompt_additions,
            )

        return sql, prompt, format_logs, error_traceback

    def question_about_data(
        self, input: str, data: pd.DataFrame, query: Optional[str] = None
    ) -> tuple[str, dict]:
        body_prompt = self.body_prompt_from_data(input=input, data=data, query=query)
        answer = call_model_direct(
            body=body_prompt,
            bedrock_runtime_client=self.bedrock_runtime_client,
            model_id=self.model_id,
        )
        return answer, body_prompt

    @track_step_and_log("🛠️ Making prompt from data and question")
    def body_prompt_from_data(
        self, input: str, data: pd.DataFrame, query: Optional[str] = None
    ) -> dict:
        prompt_data = self.dataframe_to_prompt(data=data)
        prompt = self._generate_final_answer_prompt(
            user_question=input, query=query, prompt_data=prompt_data
        )
        body_final_prompt = wrap_message_in_body(
            prompt, max_tokens=self.max_tokens, temperature=0.7, top_p=0.9
        )
        return body_final_prompt

    def ensure_is_valid_sql(self, sql: str) -> tuple[bool, str]:
        starting_words_capitalized = (
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "WITH",
            "SHOW",
        )

        def looks_like_sql_statement(sql: str) -> bool:
            sql_upper = sql.strip().upper()
            return sql_upper.startswith(starting_words_capitalized)

        try:
            sqlglot.parse_one(sql)
            assert looks_like_sql_statement(sql), (
                "Does not appear to be a full SQL statement. ",
                f"Query must start with one of {', '.join(starting_words_capitalized)}",
            )
            return True, ""
        except Exception as e:
            reason = f"⚠️ SQL invalid: {str(e)}"
            print(reason)
            return False, reason

    @track_step_and_log("🔍 Getting schemas for tables")
    def _get_schemas_for_tables(self):
        for table in self.tables:
            table.schema = get_schema_from_athena(
                athena_client=self.athena_client, table=table
            )

    @track_step_and_log(
        lambda self, attempt_number, *_, **__: f"""
        Generating SQL attempt #{str(attempt_number)}..."""
    )
    def _generate_sql(
        self, attempt_number: int, user_question: str
    ) -> tuple[str, dict, list[str], str, bool]:
        print("Generating SQL from input:", f'"{user_question}"')
        sql, prompt, format_logs, error_traceback = self.sql_prompt.generate_sql(
            user_question=user_question, tables=self.tables
        )
        is_valid_sql, reason = self.ensure_is_valid_sql(sql)
        return sql, prompt, format_logs, error_traceback, is_valid_sql

    def _generate_final_answer_prompt(
        self, user_question: str, prompt_data: str, query: Optional[str] = None
    ):

        if query is None:
            query = ""
        else:
            query = (
                "\nBased on the following query (which you shouldn't share in "
                f"the output): {query}\n"
            )

        prompt = f"""
You are a helpful data analyst assistant.
{query}
Answer the user's question/command:

"{user_question}"

Use the below data in your answer:
{prompt_data}

IF the answer contains numbers, round to sensible number of decimal places,
 include units if they exist, and choose normal units for the context.
Show the number part of the answer in bold.

DO NOT TELL US WHAT YOU DID. JUST ANSWER THE QUESTION.
"""
        return prompt
