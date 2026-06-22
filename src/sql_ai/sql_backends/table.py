from dataclasses import dataclass
from typing import Literal, Optional

SchemaType = dict[str, str]


@dataclass
class Table:
    name: str
    description: Optional[str] = None
    storage_platform: Literal["Athena", "Redshift"] = "Athena"
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

        if self.storage_platform not in ["Athena", "Redshift"]:
            raise ValueError(
                "Table.storage_platform must be either 'Athena' or 'Redshift'"
                f", got {self.storage_platform}"
            )

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
        platform = (self.storage_platform or "").lower()
        lines = [
            "###",
            f"Platform: {platform.title()}",
        ]
        if platform == "athena":
            lines.append(f"Catalog: {self.catalog}")
        lines.extend(
            [
                f"Database: {self.database}",
                f"Table: {self.name}",
                f"Description: {self.description}",
                f"Schema: {self.schema}",
                "###",
            ]
        )
        return "\n" + "\n".join(lines)
