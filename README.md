# sql-ai

Repository to generate SQL from natual langauge, with a front end chat bot UI for supplying questions!

## Instructions to Run

This code uses Poetry to manage dependencies/venvs/execution.

1. `sudo apt install python3-poetry`   # If poetry not installed on system
2. `poetry config virtualenvs.in-project true`  # Create a .venv/ folder inside the project
3. `poetry init`   # Only if pyproject.toml doesn't exist then initialise poetry
4. `poetry install`  # Install all dependencies inside pyproject.toml to the .venv/ folder
5. `poetry run pre-commit install`  # Optional, installs pre-commit hooks
6. `poetry update`  # Optional, updates all installed packages to latest allowed versions
7. `poetry lock`  # Optional, update the lock file with new package versions
8. `source .venv/bin/activate`  # Optional
9. `poetry self add poetry-plugin-export`  # Installs a plugin needed by `noxfile.py`
1-. Before pushing new code it is recommended to check code is formatted and tests pass. This can be achieved with Nox. See section below.

### Using Nox

`noxfile.py` defines multiple 'sessions' that perform actions such as linting and running tests.

These can be run individually with `nox -s <session>` e.g. `nox -s tests`

All sessions can be run together with `nox`

It is recommended to run `nox` successfully before pushing.

