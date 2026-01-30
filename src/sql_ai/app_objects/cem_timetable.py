from typing import cast

from sql_ai.config import Config
from sql_ai.sql_backend.table import Table
from sql_ai.sql_backends.athena.athena_backend import AthenaBackend
from sql_ai.sql_backends.athena.prompt_defaults import (
    ATHENA_CONTEXT_TEMPLATE,
    ATHENA_GUIDELINES,
)
from sql_ai.sql_llm import SqlLLM
from sql_ai.sql_prompting.prompting import SQLPrompt

# define table
cem_timetable_table = Table(
    name="nrs_delays_ds_schedules_daily",
    description=(
        "A table of data related to trains journey times. Each row represents when "
        "a train was at each stop on its location - this is represented by the `loc` "
        "column and the `arrival_time` and `departure_time` columns. The "
        "`start_datetime`, `end_datetime`, `origin` and `destination` columns represent "
        "the full journey. LOC and origin/destination will match (in terms of meaning) "
        "at the two ends of the journey, however the actual values in columns "
        "will be different."
    ),
    catalog="datapool_glue_datacatalog",
    database="curated-data-warehouse",
)

custom_guidelines = """
- The 'loc' column tells you where a train was at the time.
- The 'origin' and 'destination' columns tell you where the train originally departed and ultimately went on the journey
- The loc column values DO NOT match the values in 'origin' or 'destination' columns, so they can't be compared directly
- 'loc' values map to 'origin' and 'destination' values as follows:
    'york' => 'YORK'
    'newcastle' => 'NWCSTLE'
    'peterborough' => 'PBRO'
    'edinburgh' => 'EDINBUR'
    'london kings cross' => 'KNGX'
    'leeds' => 'LEEDS'
    'durham' => 'DRHM'
    'doncaster' => 'DONC'
- Be sure to allow for typos in user questions when matching location and station names
- For example, people may refer to 'london kings cross' as 'king's cross', 'kings cross', 'london kx', etc.
- For example, people often misspell 'edinburgh' as 'edinburg', 'edinbrough', 'ednburgh', etc.

- The `arrival_time`, `departure_time`, `start_datetime` and `end_datetime` columns have 'YYYY-MM-DD HH:MM:SS' format
- These columns can be converted to TIMESTAMPs with CAST(column AS TIMESTAMP)

- The `start_datetime` and `end_datetime` denote when the train was TIMETABLED to set off from origin/get to destination
- The `departure_time` and `arrival_time` denote when the train ACTUALLY departed/arrived at each `loc` station

- When determing journey durations, use the ACTUAL departure and arrival times for relevant `loc` (unless explicity asked for the timetabled duration)

KEY INSTRUCTIONS REGARDING PARTITIONING:
- The data is partitioned, to prevent dupes always add this to where clauses: CAST(partition AS DATE) = CURRENT_DATE
- The partition column should NOT be used as part of any logic related to train travel
"""  # noqa: E501


class CEMPrompt(SQLPrompt):
    def __init__(self):
        super().__init__(
            general_context_template=ATHENA_CONTEXT_TEMPLATE,
            general_guidelines=ATHENA_GUIDELINES,
        )

    def additional_guidelines(self):
        return custom_guidelines


CEMConfig = Config(
    aws_account_id="382901073838",
    aws_region="eu-west-2",
    aws_athena_s3_output_bucket="aws-athena-query-results-eu-west-2-382901073838",
    aws_profile="playground",
    bedrock_model_key="claude-sonnet-4.5",
    max_tokens=2000,
    temperature=0.9,
    aws_athena_catalog=cast(str, cem_timetable_table.catalog),
    aws_athena_database=cem_timetable_table.database,
)

CEMBackend = AthenaBackend(
    output_bucket=CEMConfig.aws_athena_s3_output_bucket,
    tables=[cem_timetable_table],
    database=CEMConfig.aws_athena_database,
    catalog=CEMConfig.aws_athena_catalog,
    aws_profile=CEMConfig.aws_profile,
    aws_region=CEMConfig.aws_region,
)

CEMLLM = SqlLLM(
    backend=CEMBackend,
    sql_prompt=CEMPrompt(),
    config=CEMConfig,
)
