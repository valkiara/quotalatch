"""Migration startup races, interrupted DDL, and changed source must be safe."""

from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

import quotalatch.ledger as ledger_module
from quotalatch import Conflict, Ledger


@pytest.fixture
def unmigrated(dsn):
    # Unlike the standard ledger fixture, this schema must not exist at test startup.
    instance = Ledger(dsn, schema=f"ql_migration_{uuid4().hex}")
    try:
        yield instance
    finally:
        with psycopg.connect(dsn) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(instance.schema))
            )


@pytest.fixture
def failing_source(tmp_path):
    source = files("quotalatch").joinpath("schema.sql").read_text(encoding="utf-8")
    # Fail only after all real migration statements, including tables and triggers.
    (tmp_path / "schema.sql").write_text(source + "\nSELECT 1 / 0;\n", encoding="utf-8")
    return tmp_path


def test_concurrent_first_migrations_initialize_schema_once(unmigrated):
    start = Barrier(8)

    def migrate(_):
        client = Ledger(unmigrated.dsn, schema=unmigrated.schema)
        start.wait(timeout=15)
        client.migrate()

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(migrate, range(8)))

    with psycopg.connect(unmigrated.dsn) as conn:
        assert conn.execute(
            sql.SQL("SELECT version FROM {}.schema_version").format(
                sql.Identifier(unmigrated.schema)
            )
        ).fetchall() == [(1,)]
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema=%s AND table_type='BASE TABLE'",
            (unmigrated.schema,),
        ).fetchall()
    assert {row[0] for row in tables} == {
        "schema_version",
        "budgets",
        "operations",
        "operation_budgets",
        "events",
    }
    # A fresh client must recognize the completed migration and be able to use it.
    fresh = Ledger(unmigrated.dsn, schema=unmigrated.schema)
    fresh.migrate()
    assert fresh.create_budget("tenant-a", "customer", 100_000).available_micro == 100_000


def test_failed_initial_migration_rolls_back_schema_and_can_retry(
    unmigrated, failing_source, monkeypatch
):
    with monkeypatch.context() as patched:
        patched.setattr(ledger_module, "files", lambda _: failing_source)
        with pytest.raises(psycopg.errors.DivisionByZero):
            unmigrated.migrate()

    with psycopg.connect(unmigrated.dsn) as conn:
        assert conn.execute("SELECT to_regnamespace(%s)", (unmigrated.schema,)).fetchone() == (
            None,
        )

    # Correcting the deployment source must work without manual cleanup or a new schema.
    fresh = Ledger(unmigrated.dsn, schema=unmigrated.schema)
    fresh.migrate()
    fresh.migrate()
    assert fresh.create_budget("tenant-a", "customer", 100_000).available_micro == 100_000


def test_changed_migration_source_is_rejected_without_changing_ledger(
    ledger, reserved, failing_source, monkeypatch
):
    before_budget = ledger.inspect_budget("tenant-a", "customer")
    before_events = ledger.events("tenant-a", reserved.operation_id)
    with monkeypatch.context() as patched:
        patched.setattr(ledger_module, "files", lambda _: failing_source)
        # A source mismatch must be detected before executing the failing DDL.
        with pytest.raises(Conflict, match="checksum"):
            ledger.migrate()

    assert ledger.inspect_budget("tenant-a", "customer") == before_budget
    assert ledger.get_operation("tenant-a", reserved.operation_id) == reserved
    assert ledger.events("tenant-a", reserved.operation_id) == before_events
    # Rejection must not overwrite the original version/checksum marker.
    ledger.migrate()
