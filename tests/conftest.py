"""
Pytest configuration and session fixtures for the Distributed Job Platform.

Guarantees Test Database Isolation:
-----------------------------------
Directs all test suites to a dedicated SQLite database (`test_job_platform.db`),
preventing synthetic test records (e.g., `test-claim-*`, `stranded-pend-*`)
from mutating or polluting the production database (`job_platform.db`).
"""

import os
from pathlib import Path
import pytest

from common.config import BASE_DIR
from common.database import init_db


@pytest.fixture(scope="session", autouse=True)
def isolated_test_database():
    """Session fixture that configures and cleans up an isolated SQLite test database."""
    test_db_name = "test_job_platform.db"
    os.environ["JOB_PLATFORM_DB_NAME"] = test_db_name
    test_db_path = BASE_DIR / test_db_name

    # Initialize tables and migrations on the test database
    init_db()

    yield test_db_path

    # Teardown: Remove temporary test database files upon test completion
    for ext in ["", "-wal", "-shm"]:
        p = BASE_DIR / f"{test_db_name}{ext}"
        if p.exists():
            try:
                p.unlink()
            except OSError:
                pass
