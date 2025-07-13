"""test athena running of queries"""

import boto3
import pytest

from sql_ai.athena.utils import (
    fetch_athena_results,
    run_query,
)
from sql_ai.utils.utils import find_aws_profile_by_account_id


def instantiate_client():
    session = boto3.Session(profile_name=find_aws_profile_by_account_id("382901073838"))
    athena_client = session.client("athena", region_name="eu-west-2")
    return athena_client


@pytest.mark.local_only
def test_fetch_athena_results_select_star():
    limit = 10
    query = f"SELECT * FROM station_lookup limit {limit}"
    rows = fetch_athena_results(
        query=query,
        client=instantiate_client(),
        database="default",
        catalog="AwsDataCatalog",
    )
    assert len(rows) == limit + 1  # for column name row
    assert len(rows[0]) == 4
    assert rows[0] == [
        "station_name",
        "three_alpha",
        "station_nlc",
        "station_name_cap",
    ]


@pytest.mark.local_only
def test_run_query():
    limit = 10
    query = f"SELECT * FROM station_lookup limit {limit}"
    df = run_query(
        query=query,
        client=instantiate_client(),
        database="default",
        catalog="AwsDataCatalog",
    )
    assert df.shape[0] == limit
    assert df.shape[1] == 4
    assert df.columns.to_list() == [
        "station_name",
        "three_alpha",
        "station_nlc",
        "station_name_cap",
    ]
