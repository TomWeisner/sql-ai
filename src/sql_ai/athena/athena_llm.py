from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional, Sequence

import boto3
import pandas as pd

from sql_ai.app_meta_objects.config import Config
from sql_ai.athena.sql_formatting.formatting import SQLFormatting, SQLFormattingOutput
from sql_ai.athena.sql_prompting import SQLPrompt
from sql_ai.athena.table import Table
from sql_ai.athena.utils import get_schema_from_athena, run_query
from sql_ai.bedrock.bedrock_llm import BedrockService, PromptBody
from sql_ai.tracking.decorator import track_step_and_log


# --- Types -------------------------------------------------------
@dataclass(frozen=True)
class SQLResult:
    sql: str
    prompt_body: Optional[PromptBody]
    format_logs: list[str]
    error_traceback: str = ""


# --- Services ----------------------------------------------------


class AthenaService:
    def __init__(self, client, output_bucket: str, tables: Sequence[Table]):
        self.client = client
        self.output_bucket = output_bucket
        self.tables = tables

    def run_query(self, query: str) -> pd.DataFrame:
        return run_query(
            query=query, client=self.client, output_bucket=self.output_bucket
        )

    @track_step_and_log("🔍 Getting schemas for tables")
    def populate_schemas(self) -> Sequence[Table]:
        for t in self.tables:
            if getattr(t, "schema", None) is None:
                t.schema = get_schema_from_athena(
                    athena_client=self.client, table=t, output_bucket=self.output_bucket
                )
        return self.tables

    def format_query(self, sql: str) -> SQLFormattingOutput:
        return SQLFormatting().format_sql(sql, tables=self.tables)


class AthenaLLM:
    def __init__(
        self,
        config: Config,
        tables: Optional[Sequence["Table"]] = None,
        sql_prompt: Optional["SQLPrompt"] = None,
        session: Optional[boto3.Session] = None,
        athena_client: Optional[object] = None,
        bedrock_runtime_client: Optional[object] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config
        self.tables: list[Table] = list(tables) if tables else []
        self.sql_prompt = sql_prompt or SQLPrompt(model=config.bedrock_model)
        self.max_sql_generation_retries = 3

        session = session or boto3.Session(profile_name=config.aws_profile)
        athena_client = athena_client or session.client(
            "athena", region_name=config.aws_region
        )
        bedrock_runtime_client = bedrock_runtime_client or session.client(
            "bedrock-runtime", region_name=config.aws_region
        )

        self.athena = AthenaService(
            athena_client, config.aws_athena_output_bucket, tables=self.tables
        )
        self.bedrock = BedrockService(bedrock_runtime_client)

        self.logger = logger or logging.getLogger(__name__)

    def get_sql(self, input: str, use_supplied_sql: bool = False) -> SQLResult:
        if not use_supplied_sql:
            self.tables = self.athena.populate_schemas()
            sql_result = self._generate_sql_with_retries(input)
        else:
            formatting_result = self.athena.format_query(sql=input)
            sql_result = SQLResult(
                sql=formatting_result.formatted_sql,
                prompt_body=None,
                format_logs=formatting_result.logs,
                error_traceback=formatting_result.error_trace,
            )
        return sql_result

    @track_step_and_log("✍️ Generating SQL")
    def _generate_sql_with_retries(self, input: str, max_retries: int = 3) -> SQLResult:
        addition = ""
        for attempt_number in range(1, max_retries + 1):
            sql, prompt_body = self.generate_sql(
                attempt_number=attempt_number,
                user_question=input + addition,
            )
            sql_formatting_result = self.athena.format_query(sql)
            if not sql_formatting_result.error_trace:
                break
            # feed validator reason back to the model for the next try
            addition = (
                "\n\nPrevious attempt error to avoid:\n"
                f"{sql_formatting_result.error_trace}\n"
            )
            time.sleep(1)

        return SQLResult(
            sql=sql_formatting_result.formatted_sql,
            prompt_body=prompt_body,
            format_logs=sql_formatting_result.logs,
            error_traceback=sql_formatting_result.error_trace,
        )

    @track_step_and_log(
        lambda self, attempt_number, *_, **__: f"""
        Attempt #{str(attempt_number)}..."""
    )
    def generate_sql(
        self, attempt_number: int, user_question: str
    ) -> tuple[str, PromptBody]:
        body: PromptBody = self.sql_prompt.build_prompt_body_for_sql(
            user_question=user_question, tables=self.tables
        )
        sql: str = self.bedrock.call(
            body=body,
            model=self.config.bedrock_model,
        )
        return sql, body

    def question_about_data(
        self, input: str, data: pd.DataFrame, query: Optional[str] = None
    ):
        body: PromptBody = self.sql_prompt.build_prompt_body_from_data(
            user_question=input, data=data
        )
        answer: str = self.bedrock.call(body=body, model=self.config.bedrock_model)
        return answer, body

    def run_athena_query(self, query: str) -> pd.DataFrame:
        return self.athena.run_query(query)
