# Redshift directory

This directory holds everything that is specific to Amazon Redshift as a backend:
- `RedshiftBackend` SQL engine implementation using the Redshift Data API.
- prompt defaults and guidelines tuned for Redshift.
- placeholder formatting steps ready for dialect-specific cleanup.

Wire it up via `build_backend(engine=\"redshift\", ...)` in `main.py`, or compose
it manually into a `SqlLLM` alongside the dataset tables you want to expose.
