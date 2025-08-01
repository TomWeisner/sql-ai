from abc import ABC

from sql_ai.athena.sql_formatting import (
    SQLFormatting,
)
from sql_ai.athena.table import Table
from sql_ai.bedrock.utils import (
    call_model_direct,
    wrap_message_in_body,
)
from sql_ai.streamlit.utils import track_step_and_log

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

    def __init__(self):
        self.formatter = SQLFormatting()

    def generate_sql(
        self, user_question, tables: list[Table], bedrock_runtime_client
    ) -> tuple[str, dict, list[str], str]:

        if len(tables) == 0:
            return "No tables found - unable to generate query.", {}, [], ""

        self.bedrock_runtime_client = bedrock_runtime_client

        tables = tables.copy()  # dont overwrite originals

        body = self.build_prompt_body(user_question, tables)
        bedrock_response = call_model_direct(
            body=body,
            model_id="anthropic.claude-3-sonnet-20240229-v1:0",
            model_version="bedrock-2023-05-31",
            bedrock_runtime_client=self.bedrock_runtime_client,
        )
        formatted_bedrock_response, formatting_logs, error_trace = (
            self.formatter.format_sql(sql=bedrock_response, tables=tables)
        )
        return formatted_bedrock_response, body, formatting_logs, error_trace

    @track_step_and_log("🛠️ Making prompt")
    def build_prompt_body(self, user_question, tables: list[Table]):
        prompt = self.general_context(user_question, tables)
        prompt += self.additional_context()
        prompt += self.general_guidelines()
        prompt += self.additional_guidelines()
        body = wrap_message_in_body(prompt, max_tokens=2000)
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
