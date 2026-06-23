# sql-ai

`sql-ai` turns natural-language questions into SQL, runs the query against a backend such as Athena or Redshift, and can turn the returned data back into a natural-language answer. It ships with a Streamlit chatbot UI and a Python API for wiring your own tables, prompts, and backends.

Supported SQL backends in this repository today:
- Athena
- Redshift

## Install from TestPyPI

This package is currently published to TestPyPI. Install it from TestPyPI, while still allowing normal dependencies to resolve from PyPI:

```bash
python -m pip install --upgrade pip
python -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple sql-ai
```

If you need a specific published build, pin the version explicitly:

```bash
python -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple "sql-ai==0.0.1.post<run-number>"
```

You will typically also need:
- Python `3.11+`
- AWS credentials/profile with access to Bedrock and your SQL backend
- An Athena query-results bucket if you are using Athena

## Use the package from Python

The package is published as `sql-ai`, but in Python you import it as `sql_ai`.

If you are importing `sql-ai` into your own project, the core building blocks are:
- `AwsConfig`
- `BedrockConfig`
- `AthenaConfig` or `RedshiftConfig`
- `Table`
- a backend such as `AthenaBackend` or `RedshiftBackend`
- `SqlLLM`

Example with Athena:

```python
from sql_ai import (
    AthenaBackend,
    AthenaConfig,
    AwsConfig,
    BedrockConfig,
    SqlLLM,
    Table,
)

table = Table(
    name="films",
    description="Pixar films and runtime metadata",
    catalog="awsdatacatalog",
    database="pixar",
)

aws_config = AwsConfig(
    profile="your-aws-profile",
    region="eu-west-2",
)

bedrock_config = BedrockConfig(
    model_key="claude-sonnet-4.6",
)

athena_config = AthenaConfig(
    output_bucket="your-athena-query-results-bucket",
    catalog=table.catalog,
    database=table.database,
)

backend = AthenaBackend(
    tables=[table],
    config=athena_config,
    aws_config=aws_config,
)

llm = SqlLLM(
    backend=backend,
    aws_config=aws_config,
    bedrock_config=bedrock_config,
)

question = "What is the average runtime of Pixar films by decade?"
sql_result = llm.get_sql(question)
print(sql_result.sql)

df = llm.run_query(sql_result.sql)
answer, _ = llm.question_about_data(question, df)
print(answer)
```

If you want to launch the packaged Streamlit chatbot UI instead of wiring it up in Python, run one of the bundled demo apps:

```bash
python -m sql_ai.main --ui streamlit --app cem
python -m sql_ai.main --ui streamlit --app pixar
```

### Using the Streamlit UI with your own objects

```python
from sql_ai.streamlit.app import ChatbotApp

app = ChatbotApp(
    athena_llm=llm,
    title="Orders analytics",
    default_question="How many orders did we receive last week?",
)
app.run()
```

Save the above as something like `my_chatbot.py`, then launch it with Streamlit:

```bash
streamlit run my_chatbot.py
```

Useful behavior to know up front:
- `SqlLLM.get_sql(...)` populates schemas, prompts Bedrock, and retries if SQL formatting/validation fails
- `SqlLLM.run_query(...)` only allows `SELECT` queries (or CTEs starting with `WITH`)
- `SqlLLM.question_about_data(...)` asks Bedrock to answer using the returned dataframe
- runtime config is explicit: pass AWS and Bedrock settings via `AwsConfig(...)` and `BedrockConfig(...)` rather than relying on environment-variable fallbacks
- if you want to change a `BedrockConfig` after construction, use `set_model_key(...)` or `set_inference_profile_id(...)` so the derived `model` stays in sync

## See the available command-line options

```bash
python -m sql_ai.main --help
```

At the time of writing, that shows these top-level options:
- `--app {cem,pixar}` to select one of the bundled demo apps
- `--ui {streamlit,cli}` to choose the web UI or terminal mode
- `--engine ENGINE` for the SQL engine key (`athena`, `redshift`); the CLI help currently notes that it is ignored if `--app` is provided

## Developer notes

Everything below is for people hacking on the repository itself rather than simply installing the package.

### Clone the repository and install dev tooling

This project uses Poetry to manage dependencies and virtual environments.

```bash
git clone https://github.com/TomWeisner/sql-ai.git
cd sql-ai
poetry config virtualenvs.in-project true
poetry install
poetry self add poetry-plugin-export
poetry run pre-commit install
```

Optional helpers:
- `poetry update` to refresh installed packages
- `poetry lock` to regenerate the lock file after dependency changes
- `source .venv/bin/activate` if you want the virtualenv activated in your shell

### Run the app from a repo checkout

From the repo root:

```bash
make chatbot
make chatbot STREAMLIT_APP=pixar
```

The first command launches the default CEM app; the second switches to the Pixar demo.

### Run checks before pushing

`noxfile.py` defines sessions for formatting, linting, typing, and tests.

Run everything:

```bash
nox
```

### Extending or debugging

- Add datasets by creating a new app object in `src/sql_ai/app_objects/` that defines tables, explicit config objects, and a prompt.
- Wire new app choices into `src/sql_ai/main.py` if you want CLI selection.
- Add a dialect by following the next section and mirroring tests under `tests/athena/` or `tests/redshift/`.
- Debug generations from the Streamlit tabs by inspecting prompts, SQL, formatting logs, and backend execution errors.

### How to add a new SQL dialect

1. Implement the backend in `src/sql_ai/sql_backends/<dialect>/<dialect>_backend.py` with a `<Dialect>Backend` that satisfies `SqlBackend`.
2. Provide prompt context and guidelines, following examples in `src/sql_ai/sql_backends/athena/prompt_defaults.py` and `src/sql_ai/sql_backends/redshift/prompt_defaults.py`.
3. Add compliance and style formatters in `src/sql_ai/sql_backends/<dialect>/sql_formatting/`.
4. Attach those formatters to the backend’s `SQLFormatting` chain.
5. Instantiate your backend via `SqlLLM(backend=..., aws_config=..., bedrock_config=...)` or through a new app object.
6. Mirror the backend tests to cover schema loading, formatting, and execution.

## Architecture overview

When `src/sql_ai/streamlit/entrypoint.py` runs, it wires together a few layers:

### App objects: `src/sql_ai/app_objects/...`

- define the tables exposed to the LLM
- bundle config such as AWS account/profile/region and Bedrock model
- customise the `SQLPrompt` for a dataset
- export a ready-to-use `SqlLLM`

### Backend selection: `SqlBackend`

- keeps orchestration backend-agnostic
- exposes schemas and metadata tables before prompting
- executes the final query against Athena or Redshift

### `SqlLLM`: `src/sql_ai/sql_llm.py`

- populates table schemas before prompting
- builds prompts and calls Bedrock
- runs SQL through dialect/style formatters
- executes the query via the selected backend
- asks Bedrock to answer using the returned data

### SQL formatting: `src/sql_ai/sql_formatting/`

- applies dialect-specific compliance fixes
- enforces SQL style conventions
- records each formatting change so the UI can show the history

### Step tracking: `src/sql_ai/tracking/`

- decorates major phases with emoji-labelled progress messages
- feeds those steps into the Streamlit sidebar

If any phase fails, `SqlLLM` retries with formatter feedback until a valid query is produced or the retry limit is hit. Once the SQL succeeds, the data is fed back into Bedrock to craft the final answer shown in the UI.

### Architecture flow

1. The user asks a question in the UI or CLI.
2. The selected backend populates table schemas.
3. `SQLPrompt` builds the SQL-generation prompt.
4. Bedrock generates SQL.
5. The backend formats and validates that SQL.
6. The backend executes the query and returns a dataframe.
7. Bedrock answers the original question using the dataframe.
