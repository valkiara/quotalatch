"""Operation lookups and journal reads participate in the caller's transaction."""

from uuid import uuid4

import psycopg
import pytest

from quotalatch import NotFound


@pytest.mark.parametrize("rollback", [False, True], ids=["commit", "rollback"])
def test_reads_see_uncommitted_reservation_without_committing_it(ledger, rollback):
    ledger.create_budget("tenant-a", "customer", 100_000)
    with psycopg.connect(ledger.dsn, autocommit=True) as conn:
        with conn.transaction(force_rollback=rollback):
            operation = ledger.reserve(
                "tenant-a",
                "queued-attempt",
                ["customer"],
                30_000,
                request_fingerprint="request:queued-attempt",
                connection=conn,
            )
            assert ledger.find_operation("tenant-a", "queued-attempt", connection=conn) == operation
            assert [
                event["kind"]
                for event in ledger.events("tenant-a", operation.operation_id, connection=conn)
            ] == ["reserved"]

            # Standalone reads still use a separate connection and cannot see the write.
            with pytest.raises(NotFound):
                ledger.find_operation("tenant-a", "queued-attempt")
            with pytest.raises(NotFound):
                ledger.events("tenant-a", operation.operation_id)

    if rollback:
        with pytest.raises(NotFound):
            ledger.find_operation("tenant-a", "queued-attempt")
        with pytest.raises(NotFound):
            ledger.events("tenant-a", operation.operation_id)
        assert ledger.inspect_budget("tenant-a", "customer").held_micro == 0
    else:
        assert ledger.find_operation("tenant-a", "queued-attempt") == operation
        assert [event["kind"] for event in ledger.events("tenant-a", operation.operation_id)] == [
            "reserved"
        ]
        assert ledger.inspect_budget("tenant-a", "customer").held_micro == 30_000


@pytest.mark.parametrize("method", ["find_operation", "events"])
def test_borrowed_read_requires_outer_transaction(ledger, method):
    identifier = "missing-key" if method == "find_operation" else uuid4()
    with psycopg.connect(ledger.dsn, autocommit=True) as conn:
        with pytest.raises(ValueError, match="explicit transaction"):
            getattr(ledger, method)("tenant-a", identifier, connection=conn)


@pytest.mark.parametrize("method", ["find_operation", "events"])
def test_missing_lookup_preserves_outer_transaction(ledger, method):
    identifier = "missing-key" if method == "find_operation" else uuid4()
    with psycopg.connect(ledger.dsn, autocommit=True) as conn, conn.transaction():
        ledger.create_budget("tenant-a", "before-lookup", 1, connection=conn)
        with pytest.raises(NotFound):
            getattr(ledger, method)("tenant-a", identifier, connection=conn)
        ledger.create_budget("tenant-a", "after-lookup", 2, connection=conn)
    assert ledger.inspect_budget("tenant-a", "before-lookup").limit_micro == 1
    assert ledger.inspect_budget("tenant-a", "after-lookup").limit_micro == 2


@pytest.mark.parametrize("method", ["find_operation", "events"])
def test_borrowed_reads_keep_tenant_scope(ledger, reserved, method):
    identifier = reserved.operation_key if method == "find_operation" else reserved.operation_id
    with psycopg.connect(ledger.dsn, autocommit=True) as conn, conn.transaction():
        with pytest.raises(NotFound):
            getattr(ledger, method)("tenant-b", identifier, connection=conn)
        result = getattr(ledger, method)("tenant-a", identifier, connection=conn)
        if method == "find_operation":
            assert result == reserved
        else:
            assert [event["kind"] for event in result] == ["reserved"]
