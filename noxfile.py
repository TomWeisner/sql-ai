import nox


nox.options.sessions = [
    "isort",
    "format",
    "format_check",
    "lint",
    "isort_check",
    "type_check",
    "tests",
]


def _run_make(session: nox.Session, target: str) -> None:
    session.run("make", target, external=True)


@nox.session(venv_backend="none", tags=["style", "fix"])
def format(session: nox.Session) -> None:
    _run_make(session, "format")


@nox.session(name="format_check", venv_backend="none", tags=["style"])
def format_check(session: nox.Session) -> None:
    _run_make(session, "format-check")


@nox.session(venv_backend="none", tags=["style"])
def lint(session: nox.Session) -> None:
    _run_make(session, "lint")


@nox.session(venv_backend="none", tags=["style", "fix"])
def isort(session: nox.Session) -> None:
    _run_make(session, "isort")


@nox.session(name="isort_check", venv_backend="none", tags=["style"])
def isort_check(session: nox.Session) -> None:
    _run_make(session, "isort-check")


@nox.session(venv_backend="none", tags=["style"])
def type_check(session: nox.Session) -> None:
    _run_make(session, "type-check")


@nox.session(venv_backend="none", tags=["test"])
def tests(session: nox.Session) -> None:
    _run_make(session, "test")


@nox.session(venv_backend="none", tags=["test"])
def coverage(session: nox.Session) -> None:
    _run_make(session, "coverage")
