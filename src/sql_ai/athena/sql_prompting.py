from abc import ABC
import pandas as pd
from sql_ai.athena.table import Table
from sql_ai.tracking.decorator import track_step_and_log
from sql_ai.bedrock.bedrock_llm import BedrockService, PromptBody
from sql_ai.bedrock.models import Model

general_context_default = """
You are an expert Athena SQL generator.

Translate the following user question into a valid Athena SQL query:

Question: {}

When doing so note:
- the `show create table <table_name>` command produces definitions of tables
- the "information_schema"."columns" table has metadata columns (and their data types) in tables. For example,
 `SELECT column_name, data_type FROM "information_schema"."columns" WHERE table_name = "<table_name>" and table_schema = "<database_name>" and table_catalog = "<catalog_name>"`
- The user is ONLY interested in data/metadata about the following tables, with schemas:
{}
"""  # noqa: E501

general_guidelines_default = """
Guidelines:
- Always wrap tables, databases and catalogs in doublequotes ("), UNLESS doing show create table then use backticks (`).
- Query ONLY from the columns, commands or databases listed above.
- Use single quotes `'` for all string literals.
- Output ONLY the SQL query (no explanation, no extra text).
- Ensure the query is valid Athena SQL syntax.
- Never use the KEYWORDS: TOP
- When selecting all columns, use the * symbol.
- When filtering to today, use the DATE_TRUNC function to truncate CURRENT_DATE to the day.
- When using the BETWEEN function, compared items must have the same datatype.
- If only table schema has been provided, this is what is by the user if they refer to "the table" or "the data"
- If the output could be a list, use bullet points.
- When adding multiple where clause conditions separate with " AND ".
- Fully qualify any selected column i.e. "catalog"."database"."table"."column"
- If asked for something like 'how many', this is likely a count query.
- DO NOT use table aliases.
- DO NOT group by using column aliases, instead use the full field expression.
- When calculating durations, always include the time unit in the column name e.g. <duration>_seconds
- When using WITH clauses, try to apply WHERE filters as early as possible i.e. inside the WITH block
"""  # noqa: E501


class SQLPrompt(ABC):

    def __init__(self, model: Model):
        self.model = model

    @track_step_and_log("🛠️ Making prompt")
    def build_prompt_body_for_sql(self, user_question, tables: list[Table]) -> PromptBody:
        prompt = self.general_context(user_question, tables)
        prompt += self.additional_context()
        prompt += self.general_guidelines()
        prompt += self.additional_guidelines()
        body = BedrockService.build_body(message=prompt, model=self.model)
        return body

    def general_context(self, user_question, tables: list[Table]) -> str:
        table_schema_context = "\n".join([table.context() for table in tables])
        return general_context_default.format(user_question, table_schema_context)

    def additional_context(self) -> str:
        return ""

    def general_guidelines(self) -> str:
        return general_guidelines_default

    def additional_guidelines(self) -> str:
        return ""

    def build_prompt_body_from_data(
        self,
        user_question: str,
        data: pd.DataFrame,
    ) -> PromptBody:
        prompt_data = BedrockService.data_to_prompt(data=data)
        prompt = (
            "You are a helpful data analyst assistant.\n"
            "Answer the user's question/command:\n\n"
            f'"{user_question}"\n\n'
            "Use the below data in your answer:\n"
            f"{prompt_data}\n\n"
            "IF the answer contains numbers, round sensibly, include units, "
            "and show the numeric part in **bold**.\n"
            "Do not describe your steps; just answer."
        )
        return BedrockService.build_body(message=prompt, model=self.model)
