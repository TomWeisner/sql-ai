from abc import ABC
from typing import Optional

import pandas as pd

from sql_ai.bedrock.bedrock_service import BedrockService, PromptBody
from sql_ai.bedrock.models import Model
from sql_ai.sql_backends.table import Table
from sql_ai.tracking.decorator import track_step_and_log


class SQLPrompt(ABC):
    def __init__(
        self,
        general_context_template: str,
        general_guidelines: str,
    ) -> None:
        self.model: Optional[Model] = None
        self.extra_context: str = ""
        self.general_context_template = general_context_template
        self.general_guidelines_text = general_guidelines

    @track_step_and_log("🛠️ Making prompt")
    def build_prompt_body_for_sql(self, user_question, tables: list[Table]) -> PromptBody:
        model = self._require_model()
        prompt = self.general_context(user_question, tables)
        prompt += self.general_guidelines()
        prompt += self.additional_guidelines()
        prompt += self.additional_context()
        body = BedrockService.build_body(message=prompt, model=model)
        return body

    def general_context(self, user_question, tables: list[Table]) -> str:
        table_schema_context = "\n".join([table.context() for table in tables])
        return self.general_context_template.format(user_question, table_schema_context)

    def additional_context(self) -> str:
        return getattr(self, "extra_context", "")

    def general_guidelines(self) -> str:
        return self.general_guidelines_text

    def additional_guidelines(self) -> str:
        return ""

    def build_prompt_body_from_data(
        self,
        user_question: str,
        data: pd.DataFrame,
    ) -> PromptBody:
        model = self._require_model()
        prompt_data = BedrockService.data_to_prompt(data=data)
        extra_context = self.extra_context or ""
        prompt = (
            "You are a helpful data analyst assistant.\n"
            "You are interpreting the results of a SQL query to answer the user's "
            "latest question.\n"
            "Use only the data provided and the conversation context (if supplied).\n\n"
            f"{extra_context}\n"
            "Most recent user question:\n"
            f'"{user_question}"\n\n'
            "SQL query results:\n"
            f"{prompt_data}\n\n"
            "If the answer contains numbers, round sensibly, include units, "
            "and show the numeric part in **bold**.\n"
            "Do not describe your steps; just answer."
        )
        return BedrockService.build_body(message=prompt, model=model)

    def _require_model(self) -> Model:
        if self.model is None:
            raise ValueError("SQLPrompt.model has not been set from Config.")
        return self.model
