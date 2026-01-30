from sql_ai.sql_backend.table import Table
from sql_ai.streamlit.validators import get_storage_platform_error


def test_get_storage_platform_error_none_when_empty():
    assert get_storage_platform_error([]) is None


def test_get_storage_platform_error_none_for_single_platform():
    table = Table(name="t1", description="desc", storage_platform="Athena")
    assert get_storage_platform_error([table]) is None


def test_get_storage_platform_error_for_mixed_platforms():
    table1 = Table(name="t1", description="desc", storage_platform="Athena")
    table2 = Table(name="t2", description="desc", storage_platform="Redshift")
    message = get_storage_platform_error([table1, table2])
    assert message is not None
    assert "Athena" in message
    assert "Redshift" in message
