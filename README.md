# sql-ai

Repository to generate SQL from natual langauge, with a front end chat bot for supplying questions.

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

## How to run the chatbot

First, set up venv and intall requirements. See section above.

Then, in a terminal make your working directory the root of the project and run:

`streamlit run src/sql/_ai/streamlit/app.py`


# Athena

The `src/sql_ai/athena` directory is concerned with running LLMs on structured data stored in Athena databases

The AthenaLLM class handles input questions to return an Athena compliant query

It beings by building a prompt from the:
1. input question
2. schema of tables supplied to the chatbot (which can include the official data defintion Comments related to Athena columns and overall tables)

The Bedrock model in use is then called to generate a SQL query.

The generated SQL query is passed through various Formatting steps:
- Fixing (making Athena compliant)
- Standardising (prettifying with standard spacings etc.)

This SQL is then ran on Athena.

Assuming successful return of an answer, the AthenaLLM class converts the returned data to a string and 
uses this as context to a re run of the LLM.

If the SQL generation/use fails then the process repeats until either max retries is
reached or the generated query succeeds in returning data.

## Class relationships

![Athena LLM classes](classes.excalidraw.png)

## Answering user questions

Athena LLM class process flow:
![Athena LLM class](athena_llm.excalidraw.png)

## Building SQL

SQL Prompt class process flow:
![SQL Prompt class](sql_prompting.excalidraw.png)
