# sql-ai

Repository to generate SQL from natural language, with a Streamlit chatbot front end.

The repository is designed to be SQL type agnostic, in the sense the core framework does not
depend on a specific SQL type. Of course, the generated SQL must be of a specific type, and thus
the code enables different SQL types to be defined as _protocols_, which can then be used by the
framework.

Currently protocols are defined for:
- Athena
- Redshift

It is possible to add to this repository protocols for other types. Give it a go!

## How to use/update the repo

This project uses Poetry to manage dependencies/venvs/execution.

1. `sudo apt install python3-poetry`   # If poetry not installed on system
2. `poetry config virtualenvs.in-project true`  # Create a .venv/ folder inside the project
3. `poetry init`   # Only if pyproject.toml doesn't exist then initialise poetry
4. `poetry install`  # Install all dependencies inside pyproject.toml to the .venv/ folder
5. `poetry run pre-commit install`  # Optional, installs pre-commit hooks
6. `poetry update`  # Optional, updates all installed packages to latest allowed versions
7. `poetry lock`  # Optional, update the lock file with new package versions
8. `source .venv/bin/activate`  # Optional
9. `poetry self add poetry-plugin-export`  # Installs a plugin needed by `noxfile.py`
10. Before pushing new code it is recommended to check code is formatted and tests pass. This can be achieved with Nox...

`noxfile.py` defines multiple 'sessions' that perform actions such as linting and running tests.

These can be run individually with `nox -s <session>` e.g. `nox -s tests`

All sessions can be run together with `nox`

It is recommended to run `nox` successfully before pushing.

## How to add a new SQL dialect
1. Implement the backend: create `src/sql_ai/<dialect>/<dialect>_backend.py` with a `<Dialect>Backend` that satisfies `SqlBackend` (clients, metadata/schemas, execution).
2. Provide prompts: add context/guidelines (see `athena/prompt_defaults.py`, `redshift/prompt_defaults.py`).
3. Add formatting: create compliance and style formatters (i.e `src/sql_ai/<dialect>/sql_formatting/<dialect>_compliance.py` and `src/sql_ai/<dialect>/sql_formatting/<dialect>_style_standards_.py`).
4. Attach them to the backend’s `SQLFormatting` chain, so generated SQL is cleaned for your dialect.
5. Instantiate and use: pass your backend into `SqlLLM(config=..., backend=...)` (or create an app object that does this).
6. Test it: mirror the backend tests (e.g. `tests/athena/`, `tests/redshift/`).

## How to run the chatbot

Enter venv, then:

`streamlit run src/sql_ai/streamlit/app.py`


## Architecture overview

When `streamlit/app.py` runs it wires together a few layers:

App objects: `src/sql_ai/app_objects/...`
- tables: define which tables (with associated catalog/database/name/description/schema) are exposed
- config: bundle environment choices (AWS account/profile/region, Bedrock model, backend connection details)
- prompt: customise the `SQLPrompt` for the dataset
- llm: export a ready-to-use `SqlLLM` for the UI

Backend selection: `SqlBackend` protocol
- engines: pick an engine (Athena/Redshift) while keeping orchestration agnostic
- metadata: surface backend-provided schemas and system tables before prompting

SqlLLM: `src/sql_ai/sql_llm.py`
- schemas: populate table schemas before prompting
- prompting: build prompts and call Bedrock
- formatting: run SQL through dialect/style fixers
- execution: run the query via the selected backend
- answers: ask Bedrock to respond using the returned data

SQL formatting: `src/sql_ai/sql_formatting/`
- compliance: apply dialect-specific fixes
- style: enforce spacing/JOIN/style conventions
- logging: record every change so the UI can show the history

Dialect backends: `src/sql_ai/athena/`, `src/sql_ai/redshift/`
- clients: handle engine clients and connections
- metadata: fetch schemas and metadata tables
- execution: submit queries and stream results
- prompts: supply dialect defaults for prompt building
- formatting: provide dialect-specific rules used by `SQLFormatting`

Step tracking: `src/sql_ai/tracking/`
- wrapping: decorate major phases with emoji-labelled progress messages
- UI: feed those steps into the Streamlit sidebar

If any phase fails (formatting error, backend validation, etc.), `SqlLLM` retries with the formatter’s feedback until a valid query is produced or the retry limit is hit. Once the SQL succeeds, the data is fed back into Bedrock to craft the final answer shown in the UI.***
