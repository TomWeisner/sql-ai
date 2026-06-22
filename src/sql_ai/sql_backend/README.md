### SQL Backend compatibility layer

The shared backend primitives now live in `sql_ai.sql_backends` alongside the
engine-specific implementations.

This directory remains in place only as a lightweight compatibility layer so
older imports such as `sql_ai.sql_backend.table` and `sql_ai.sql_backend.base`
continue to work.
