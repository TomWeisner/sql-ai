import boto3

from sql_ai.athena.athena_llm import AthenaLLM
from sql_ai.athena.sql_prompting import (
    SQLPrompt,
)
from sql_ai.athena.table import Table
from sql_ai.utils.utils import find_aws_profile_by_account_id

# AWS session and clients
session = boto3.Session(profile_name=find_aws_profile_by_account_id("382901073838"))
athena = session.client("athena", region_name="eu-west-2")
bedrock_runtime = session.client("bedrock-runtime", region_name="eu-west-2")

cem_timetable_table = Table(
    name="nrs_delays_ds_schedules_daily",
    description=(
        "A table of data related to trains journey times. Each row represents "
        "when a train was at each stop on its location - this is represented by "
        "the `loc` column and the `arrival_time` and `departure_time` columns. The "
        "`start_datetime`, `end_datetime`, `origin` and `destination` columns "
        "represent the full journey. LOC and origin/destination will match (in terms "
        "of meaning) at the two ends of the journey, jowever the actual values will "
        "be different."
    ),
    catalog="datapool_glue_datacatalog",
    database="curated-data-warehouse",
    schema=[
        "bs_train_uid (string)",
        "headcode (string)",
        "rsid (string)",
        "origin (string)",
        "start_datetime (string)",
        "destination (string)",
        "end_datetime (string)",
        "loc (string)",
        "arrival_time (string)",
        "departure_time (string)",
        "date (date)",
        "partition (string - partitioned)",
    ],
)

custom_guidelines = """
- The 'loc' column tells you where a train was at the time. The 'origin' and 'destination' columns tell you where the train originally departed and ultimately went
- The loc column values DO NOT match the values in 'origin' or 'destination' columns, so they can't be compared
- Location mapping:
    'york' => 'YORK'
    'newcastle' => 'NWCSTLE'
    'peterborough' => 'PBRO'
    'edinburgh' => 'EDINBUR'
    'london kings cross' => 'KNGX'
    'leeds' => 'LEEDS'
    'durham' => 'DRHM'
    'doncaster' => 'DONC'
- The `arrival_time`, `departure_time`, `start_datetime` and `end_datetime` columns are strings in 'YYYY-MM-DD HH:MM:SS' format
- The above columns can be converted to TIMESTAMPs with CAST(column AS TIMESTAMP)
- The `start_datetime` and `end_datetime` denote when the train was TIMETABLED to set off from origin/get to destination
- The `departure_time` and `arrival_time` denote when the train ACTUALLY departed/arrived at each `loc` station
- When determing journey durations, use the actual departure and arrival times for relevant `loc` (unless explicity asked for the timetabled duration)
- The data is partitioned, to prevent dupes you always need to add this to the where clauses: CAST(partition AS DATE) = CURRENT_DATE
- The partition column has nothing to do with the data so should NOT be used as part of any logic related to train travel
"""  # noqa: E501


class CEMTimetablePrompt(SQLPrompt):
    def __init__(self, bedrock_runtime_client=bedrock_runtime):
        super().__init__(bedrock_runtime_client=bedrock_runtime_client)

    def additional_guidelines(self):
        return custom_guidelines


CEMTimetableLLM = AthenaLLM(
    tables=[cem_timetable_table],
    athena_client=athena,
    bedrock_runtime_client=bedrock_runtime,
    sql_prompt=CEMTimetablePrompt(bedrock_runtime_client=bedrock_runtime),
)
