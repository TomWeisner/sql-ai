from dataclasses import dataclass, field
from typing import Optional, Union


@dataclass
class Table:
    name: str
    description: Optional[str] = None
    catalog: Optional[str] = "awsdatacatalog"
    database: str = "default"
    schema: Union[dict[str, str], list[str]] = field(default_factory=dict)

    def __post_init__(self):
        if not self.description:
            raise ValueError("Table must have a description")
        if self.catalog is None:
            self.catalog = "awsdatacatalog"

        if isinstance(self.schema, list):
            dict_schema = {}
            for column in self.schema:
                column = column.strip()
                if "(" not in column or not column.endswith(")"):
                    raise ValueError(
                        f"Invalid format: '{column}', expecting 'column_name (data_type)'"
                    )
                name, datatype = column[:-1].split("(", 1)
                dict_schema[name.strip()] = datatype.strip()
            self.schema = dict_schema

    def qualified_name(self):
        """athena query syntax"""
        # when dealing with WITH tables we will use _ for the database
        if self.database == "_":
            return self.name
        # when dealing with metadata queries against information_schema
        # we will use _ for the catalog
        elif self.catalog == "_":
            return f'"{self.database}"."{self.name}"'
        return f'"{self.catalog}"."{self.database}"."{self.name}"'

    def qualified_name_hive_syntax(self):
        """hive query syntax - used when running `show create table...`"""
        return f"`{self.catalog}`.`{self.database}`.`{self.name}`"

    def context(self):
        return (
            f"###\nCatalog: {self.catalog}\nDatabase: {self.database}\nTable: {self.name}"
            f'\nDescription: {self.description}\nSchema: {self.schema}\n#####"'
        )
