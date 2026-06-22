"""Compatibility exports for the consolidated backend package."""

from sql_ai.sql_backends.base import SqlBackend
from sql_ai.sql_backends.table import SchemaType, Table

__all__ = ["SchemaType", "SqlBackend", "Table"]
