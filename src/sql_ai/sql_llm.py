import logging
import time
from dataclasses import dataclass
from typing import Optional

import boto3
import pandas as pd
from botocore.client import BaseClient

from sql_ai.bedrock.bedrock_service import BedrockService, PromptBody
from sql_ai.config import Config
from sql_ai.sql_backends import SqlBackend, Table
from sql_ai.sql_prompting.prompting import SQLPrompt
from sql_ai.tracking.decorator import track_step_and_log


@dataclass(frozen=True)
class SQLResult:
    sql: str
    prompt_body: Optional[PromptBody]
    format_logs: list[str]
    error_traceback: str = ""


class SqlLLM:
    def __init__(
        self,
        config: Config,
        backend: SqlBackend,
        sql_prompt: Optional[SQLPrompt] = None,
        bedrock_runtime_client: Optional[BaseClient] = None,
        session: Optional[boto3.Session] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config
        self.backend = backend
        self.tables: list[Table] = backend.tables
        if sql_prompt:
            self.sql_prompt = sql_prompt
        else:
            self.sql_prompt = SQLPrompt(
                general_context_template=backend.prompt_context_template,
                general_guidelines=backend.prompt_guidelines,
            )
        self.sql_prompt.general_context_template = backend.prompt_context_template
        self.sql_prompt.general_guidelines_text = backend.prompt_guidelines
        self.sql_prompt.model = config.bedrock_model
        self.max_sql_generation_retries = 3

        session = session or boto3.Session(profile_name=config.aws_profile)
        bedrock_runtime_client = bedrock_runtime_client or session.client(
            "bedrock-runtime", region_name=config.aws_region
        )

        self.bedrock = BedrockService(bedrock_runtime_client)
        self.logger = logger or logging.getLogger(__name__)

    def get_sql(self, input: str, use_supplied_sql: bool = False) -> SQLResult:
        if not use_supplied_sql:
            self.tables = list(self.backend.populate_schemas())
            sql_result = self._generate_sql_with_retries(input)
        else:
            formatting_result = self.backend.format_query(sql=input)
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
            sql_formatting_result = self.backend.format_query(sql)
            if not sql_formatting_result.error_trace:
                break
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
        lambda self, attempt_number, *_, **__: f"""Attempt #{str(attempt_number)}..."""
    )
    def generate_sql(
        self, attempt_number: int, user_question: str
    ) -> tuple[str, PromptBody]:
        body: PromptBody = self.sql_prompt.build_prompt_body_for_sql(
            user_question=user_question, tables=self.tables
        )
        sql: str = self.bedrock.call(body=body, model=self.config.bedrock_model)
        return sql, body

    def question_about_data(
        self, input: str, data: pd.DataFrame, query: Optional[str] = None
    ):
        body: PromptBody = self.sql_prompt.build_prompt_body_from_data(
            user_question=input, data=data
        )
        answer: str = self.bedrock.call(body=body, model=self.config.bedrock_model)
        return answer, body

    def run_query(self, query: str) -> pd.DataFrame:
        cleaned = self._clean_query_prefix(query)
        self._assert_select_query(cleaned)
        return self.backend.run_query(cleaned)

    @staticmethod
    def _assert_select_query(query: str) -> None:
        """Require queries to begin with SELECT or WITH (for CTEs)."""
        stripped = query.lstrip().lstrip("`(")
        lowered = stripped.lower()
        if not lowered.startswith(("select", "with")):
            raise ValueError(
                "Only SELECT statements are allowed (CTEs starting with WITH are fine). "
                "Received query:\n"
                f"{query}"
            )

    @staticmethod
    def _clean_query_prefix(query: str) -> str:
        """
        Remove common leading wrappers like backticks and a leading 'sql' token.
        """
        cleaned = query.strip()
        # strip enclosing backticks or quotes
        if cleaned.startswith(("`", '"')) and cleaned.endswith(("`", '"')):
            cleaned = cleaned[1:-1].strip()
        lowered = cleaned.lower()
        if lowered.startswith("sql"):
            cleaned = cleaned[3:].lstrip(" :\n\t")
        return cleaned
