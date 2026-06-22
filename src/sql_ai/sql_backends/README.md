# SQL backends

This package now contains the full SQL backend namespace for the project.

- `base.py` defines the shared `SqlBackend` protocol.
- `table.py` holds the shared `Table` metadata object.
- `athena/` contains Athena-specific implementation, formatting, and prompt defaults.
- `redshift/` contains Redshift-specific implementation, formatting, and prompt defaults.

For backward compatibility, `sql_ai.sql_backend` remains as a thin wrapper that
re-exports the shared primitives from this package.