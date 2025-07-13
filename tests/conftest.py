# tests/conftest.py
import os
import sys
from pathlib import Path

import pytest

# Add `src/` and `tests/` to sys.path if not already present
BASE_DIR = Path(__file__).resolve().parent.parent
for subdir in ["src", "tests"]:
    path = str(BASE_DIR / subdir)
    if path not in sys.path:
        sys.path.insert(0, path)


def pytest_runtest_setup(item):
    if "local_only" in item.keywords and os.getenv("CI") == "true":
        pytest.skip("Skipping local-only test in CI environment")
