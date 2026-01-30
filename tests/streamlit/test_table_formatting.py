from sql_ai.streamlit.table_formatting import format_table_description


def test_format_table_description_wraps_code():
    text = "Uses `col_one` and `col_two` for filtering."
    html = format_table_description(text)
    assert "<code>col_one</code>" in html
    assert "<code>col_two</code>" in html
    assert "`" not in html
