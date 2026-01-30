from sql_ai.sql_formatting.formatter_base import SQLFormatter


class SQLRedshiftCompliance(SQLFormatter):
    """Placeholder compliance step for Redshift-specific SQL adjustments."""

    def __init__(self, nickname: str = "🧐 Ensuring Redshift compliance") -> None:
        super().__init__(nickname=nickname)
        # Add Redshift-specific cleaning methods here as they are discovered.
        self.methods = []
