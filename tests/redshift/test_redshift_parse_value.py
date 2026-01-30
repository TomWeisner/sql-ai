from sql_ai.sql_backends.redshift.redshift_backend import RedshiftBackend


def test_parse_value_handles_nulls():
    assert RedshiftBackend._parse_value({}) is None
    assert RedshiftBackend._parse_value({"isNull": True}) is None


def test_parse_value_handles_known_types():
    assert RedshiftBackend._parse_value({"stringValue": "abc"}) == "abc"
    assert RedshiftBackend._parse_value({"longValue": 5}) == 5
    assert RedshiftBackend._parse_value({"doubleValue": 1.5}) == 1.5
    assert RedshiftBackend._parse_value({"booleanValue": True}) is True


def test_parse_value_falls_back_to_any_value():
    assert RedshiftBackend._parse_value({"blobValue": "xyz"}) == "xyz"
