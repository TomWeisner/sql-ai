from sql_ai.streamlit.sql_utils import normalize_sql


class DummyLLM:
    def __init__(self) -> None:
        self.seen: str | None = None

    def _clean_query_prefix(self, value: str) -> str:
        self.seen = value
        return value


def test_normalize_sql_strips_code_fence_and_sql_prefix():
    llm = DummyLLM()
    sql = """```sql\nSELECT 1\n```"""
    normalized = normalize_sql(llm, sql)
    assert normalized == "SELECT 1"
    assert llm.seen == "SELECT 1"


def test_normalize_sql_passes_through_plain_sql():
    llm = DummyLLM()
    normalized = normalize_sql(llm, "SELECT 2")
    assert normalized == "SELECT 2"
    assert llm.seen == "SELECT 2"
