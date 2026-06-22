from sql_ai.sql_formatting.formatter_base import SQLFormatter


class SQLRedshiftStandards(SQLFormatter):
    """Placeholder style rules for Redshift queries."""

    def __init__(self, nickname: str = "🧹 Applying SQL standards") -> None:
        super().__init__(nickname=nickname)
        # Add style enforcement steps for Redshift as they are defined.
        self.methods = []
