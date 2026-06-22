# Athena directory

This directory holds everything that is specific to AWS Athena as a backend:
- allowed objects lists
- formatting rules
- prompt defaults
- `AthenaBackend` sql engine implementation.

If you need to tweak how we talk to Athena (different regions, compliance rules, etc.)
this is the place to do it.  
It intentionally contains no app logic; the rest of the project consumes Athena through
the backend abstraction in `sql_backends`.
