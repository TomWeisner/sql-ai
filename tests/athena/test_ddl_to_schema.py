from unittest.mock import MagicMock

from sql_ai.sql_backends.athena.athena_backend import AthenaBackend


def test_ddl_to_schema_handles_no_parentheses():
    backend = AthenaBackend(output_bucket="bucket", client=MagicMock())
    ddl = "`bs_train_uid` string, `headcode` string, `date` date)"
    schema = backend._ddl_to_schema(ddl)
    assert schema["bs_train_uid"] == "string"
    assert schema["headcode"] == "string"
    assert schema["date"] == "date"
