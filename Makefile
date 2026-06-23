# Declare targets that don't correspond to real files so Make always runs them.
.PHONY: install format format-check lint local-lint isort isort-check type-check test coverage coverage-artifacts ci precommit chatbot

POETRY ?= poetry
PYTHONPATH := src:tests
SOURCE_DIRS := src tests
STREAMLIT_APP ?= cem

install:
	@echo "Installing dependencies..."
	$(POETRY) install --no-interaction

format:
	$(POETRY) run black $(SOURCE_DIRS)

format-check:
	$(POETRY) run black --check $(SOURCE_DIRS)

lint:
	$(POETRY) run flake8 --max-line-length=90 $(SOURCE_DIRS)

local-lint: lint

isort:
	$(POETRY) run isort $(SOURCE_DIRS)

isort-check:
	$(POETRY) run isort --check-only $(SOURCE_DIRS)

type-check:
	$(POETRY) run mypy --explicit-package-bases src tests

test:
	PYTHONPATH=$(PYTHONPATH) $(POETRY) run pytest tests

coverage:
	PYTHONPATH=$(PYTHONPATH) $(POETRY) run pytest tests --cov=sql_ai --cov-report=xml --cov-report=term-missing

coverage-artifacts:
	ls -al
	find . -maxdepth 3 \( -name "coverage.xml" -o -name ".coverage*" \)

ci:
	$(MAKE) format-check
	$(MAKE) lint
	$(MAKE) isort-check
	$(MAKE) type-check
	$(MAKE) coverage

precommit:
	$(POETRY) run pre-commit run --all-files

chatbot:
	$(POETRY) run streamlit run src/sql_ai/streamlit/entrypoint.py -- $(STREAMLIT_APP)