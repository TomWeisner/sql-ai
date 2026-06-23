from typing import cast

from sql_ai.config import AthenaConfig, AwsConfig, BedrockConfig
from sql_ai.sql_backends.athena.athena_backend import AthenaBackend
from sql_ai.sql_backends.athena.prompt_defaults import (
    ATHENA_CONTEXT_TEMPLATE,
    ATHENA_GUIDELINES,
)
from sql_ai.sql_backends.table import Table
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
In the `nrs_delays_ds_schedules_daily` table:
- The 'loc' column tells you where a train was at the time.
- The 'origin' and 'destination' columns tell you where the train originally departed and ultimately went on the journey
- Actual cities map to 'loc', 'origin' and 'destination' column values as follows:
    'York' => 'YORK'
    'Newcastle' => 'NWCSTLE'
    'Peterborough' => 'PBRO'
    'Edinburgh' => 'EDINBUR'
    'London Kings Cross' => 'KNGX'
    'Leeds' => 'LEEDS'
    'Durham' => 'DRHM'
    'Doncaster' => 'DONC'
- Be sure to allow for typos in user questions when matching location and station names
- For example, people may refer to 'london kings cross' as 'king's cross', 'kings cross', 'london kx', 'kgx', etc.
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


CEMAwsConfig = AwsConfig(
    account_id="382901073838",
    region="eu-west-2",
    profile="playground",
)

CEMBedrockConfig = BedrockConfig(
    model_key="claude-sonnet-4.6",
    max_tokens=2000,
    temperature=0.9,
)

CEMAthenaConfig = AthenaConfig(
    output_bucket="aws-athena-query-results-eu-west-2-382901073838",
    catalog=cast(str, cem_timetable_table.catalog),
    database=cem_timetable_table.database,
)

CEMBackend = AthenaBackend(
    tables=[cem_timetable_table],
    config=CEMAthenaConfig,
    aws_config=CEMAwsConfig,
)

CEMLLM = SqlLLM(
    backend=CEMBackend,
    sql_prompt=CEMPrompt(),
    aws_config=CEMAwsConfig,
    bedrock_config=CEMBedrockConfig,
)
