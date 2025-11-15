ATHENA_CONTEXT_TEMPLATE = """
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

ATHENA_GUIDELINES = """
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
- Never wrap a query in encapsulating quotes, just return the query itself!
"""  # noqa: E501
