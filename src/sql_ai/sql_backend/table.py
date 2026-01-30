from dataclasses import dataclass
from typing import Optional

SchemaType = dict[str, str]


@dataclass
class Table:
    name: str
    description: Optional[str] = None
    catalog: Optional[str] = "awsdatacatalog"
    database: str = "default"
    schema: Optional[SchemaType] = None

    def __post_init__(self):
        if not self.description:
            raise ValueError("Table must have a description")
        if self.catalog is None:
            self.catalog = "awsdatacatalog"

        if self.schema is not None and not isinstance(self.schema, dict):
            raise TypeError("Table.schema must be a dict mapping column -> datatype.")

    def qualified_name(self):
        """dialect-specific qualified identifier."""
        if self.database == "_":
            return self.name
        if self.catalog == "_":
            return f'"{self.database}"."{self.name}"'
        return f'"{self.catalog}"."{self.database}"."{self.name}"'

    def qualified_name_hive_syntax(self):
        """Hive query syntax - used when running `show create table...`."""
        return f"`{self.catalog}`.`{self.database}`.`{self.name}`"

    def context(self):
        return (
            f"\n###\nCatalog: {self.catalog}"
            f"\nDatabase: {self.database}"
            f"\nTable: {self.name}"
            f"\nDescription: {self.description}"
            f"\nSchema: {self.schema}\n###"
        )
