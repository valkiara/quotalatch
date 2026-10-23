import os
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from quotalatch import Ledger


@pytest.fixture(scope="session")
def dsn():
    value = os.environ.get("BUDGET_DATABASE_URL")
    if not value:
        pytest.fail("Set BUDGET_DATABASE_URL to a PostgreSQL test database; see README.md")
    with psycopg.connect(value, connect_timeout=5) as conn:
        conn.execute("SELECT 1")
    return value


@pytest.fixture
def ledger(dsn):
    schema = f"ql_test_{uuid4().hex}"
    instance = Ledger(dsn, schema=schema)
    instance.migrate()
    try:
        yield instance
    finally:
        # Only the random schema created by this fixture is removed.
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def reserved(ledger):
    ledger.create_budget("tenant-a", "customer", 1_000_000)
    return ledger.reserve(
        "tenant-a",
        "job:attempt:1",
        ["customer"],
        300_000,
        request_fingerprint="canonical-request-digest",
    )


@pytest.fixture
def dispatched(ledger, reserved):
    return ledger.mark_dispatched(
        "tenant-a",
        reserved.operation_id,
        reserved.dispatch_token,
    )


def expire(ledger, operation_id):
    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL(
                "UPDATE {}.operations SET expires_at=clock_timestamp()-interval '1 second' "
                "WHERE operation_id=%s"
            ).format(sql.Identifier(ledger.schema)),
            (operation_id,),
        )
