from sql_ai.sql_backend.table import Table


def test_table_qualified_name_full():
    table = Table(name="my_table", description="desc", catalog="cat", database="db")
    assert table.qualified_name() == '"cat"."db"."my_table"'


def test_table_qualified_name_no_catalog():
    table = Table(name="my_table", description="desc", catalog="_", database="db")
    assert table.qualified_name() == '"db"."my_table"'


def test_table_qualified_name_no_database():
    table = Table(name="my_table", description="desc", catalog="cat", database="_")
    assert table.qualified_name() == "my_table"


def test_table_context_includes_platform_and_schema():
    table = Table(
        name="my_table",
        description="desc",
        catalog="cat",
        database="db",
        schema={"col": "string"},
    )
    context = table.context()
    assert "Platform: Athena" in context
    assert "Catalog: cat" in context
    assert "Database: db" in context
    assert "Schema" in context
