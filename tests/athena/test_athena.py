from sql_ai.athena.utils import fetch_athena_results, run_query


def test_fetch_athena_results_select_star(mock_athena_client):
    rows = fetch_athena_results(
        query="SELECT * FROM station_lookup LIMIT 10",
        client=mock_athena_client,
        database="default",
        catalog="AwsDataCatalog",
    )

    assert len(rows) == 11  # 1 header + 10 rows
    assert rows[0] == [
        "station_name",
        "three_alpha",
        "station_nlc",
        "station_name_cap",
    ]


def test_run_query(mock_athena_client):
    df = run_query(
        query="SELECT * FROM station_lookup LIMIT 10",
        client=mock_athena_client,
        database="default",
        catalog="AwsDataCatalog",
    )

    assert df.shape == (10, 4)
    assert df.columns.tolist() == [
        "station_name",
        "three_alpha",
        "station_nlc",
        "station_name_cap",
    ]
