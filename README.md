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

## For analysts (using the app)
- Launch the UI: `streamlit run src/sql_ai/streamlit/app.py` from the repo root.
- Ask questions: type a natural-language question in the Streamlit input; the app generates SQL, executes it, and shows an answer.
- Inspect the result: expand the tabs to see the SQL, formatting changes, data, and prompts that drove the answer.
- Export: use the download button in the “Data” tab to export the result set as CSV.
- Troubleshoot: if you see an error, check the displayed SQL and formatting logs; retry after adjusting your question.

## For engineers (extending or debugging)
- Run checks: `nox` or individual sessions (e.g., `nox -s lint`, `nox -s tests`).
- Add datasets: create a new app object in `src/sql_ai/app_objects/` that defines tables, config, and a prompt; wire it into `main.py` if you need CLI selection.
- Add a dialect: follow “How to add a new SQL dialect” to implement a backend, prompts, and formatting rules.
- Debug generations: use the Streamlit tabs to inspect prompts/SQL/formatting; check backend errors for the executed SQL.
- Tests: mirror backend tests (see `tests/athena/`, `tests/redshift/`) when adding engines or changing execution paths.

## How to add a new SQL dialect
1. Implement the backend: create `src/sql_ai/<dialect>/<dialect>_backend.py` with a `<Dialect>Backend` that satisfies `SqlBackend` (clients, metadata/schemas, execution).
2. Provide prompts: add context/guidelines (see `sql_backends/athena/prompt_defaults.py`, `sql_backends/redshift/prompt_defaults.py`).
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

Dialect backends: `src/sql_ai/sql_backends/athena/`, `src/sql_ai/sql_backends/redshift/`
- clients: handle engine clients and connections
- metadata: fetch schemas and metadata tables
- execution: submit queries and stream results
- prompts: supply dialect defaults for prompt building
- formatting: provide dialect-specific rules used by `SQLFormatting`

Step tracking: `src/sql_ai/tracking/`
- wrapping: decorate major phases with emoji-labelled progress messages
- UI: feed those steps into the Streamlit sidebar

If any phase fails (formatting error, backend validation, etc.), `SqlLLM` retries with the formatter’s feedback until a valid query is produced or the retry limit is hit. Once the SQL succeeds, the data is fed back into Bedrock to craft the final answer shown in the UI.

### Architecture flow

```mermaid
A - User question via Streamlit UI
B - Declare tables: Table(name="demo", database="db", catalog="cat", description="Mock table")
C - Init backend: AthenaBackend(output_bucket="s3://bucket", ...)
D - Populate schemas: backend.populate_schemas()
E - Build SQL prompt: sql_prompt.build_prompt_body_for_sql(question, tables)
F - Generate SQL: SqlLLM.generate_sql(question) -> Bedrock
G - Raw SQL text from Bedrock
H - Format/validate SQL: backend.format_query(sql)
I - Execute query: backend.run_query(formatted_sql) -> DataFrame
J - Answer from data: SqlLLM.question_about_data(question, df) -> Bedrock
K - UI shows steps / SQL / logs / data / answer
```
