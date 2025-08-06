import nox_poetry


# Linting
@nox_poetry.session(tags=["style"])
def lint(session: nox_poetry.Session) -> None:
    session.install("flake8", "flake8-black")
    session.run("flake8", "--max-line-length=90", "src", "tests")


# Isort
@nox_poetry.session(tags=["style", "fix"])
def isort(session: nox_poetry.Session) -> None:
    session.install("isort")
    session.run("isort", "src", "tests")


# Formatting
@nox_poetry.session(tags=["style", "fix"])
def format(session: nox_poetry.Session) -> None:
    session.install("black")
    session.run("black", "src", "tests")


# Type checking
@nox_poetry.session(tags=["style", "fix"])
def type_check(session: nox_poetry.Session) -> None:
    session.install(".")
    session.run("mypy", "--explicit-package-bases", "src", "tests", external=True)


# Tests
@nox_poetry.session(tags=["test"])
def tests(session: nox_poetry.Session) -> None:
    session.install("pytest")
    session.install(".")
    session.run("pytest", "tests", env={"PYTHONPATH": "src:tests"})
