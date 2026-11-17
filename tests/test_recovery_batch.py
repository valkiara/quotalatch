"""Recovery can resume after a write failure partway through a batch."""

import psycopg
import pytest
from psycopg import sql

from quotalatch import Ledger, Status


def test_recovery_batch_preserves_committed_progress_and_retries_remaining_work(ledger):
    tenant = "tenant-a"
    ledger.create_budget(tenant, "customer", 100_000)
    first, second, third = [
        ledger.reserve(tenant, key, ["customer"], amount, request_fingerprint=key)
        for key, amount in (("first", 30_000), ("second", 40_000), ("third", 20_000))
    ]
    ledger.mark_dispatched(tenant, second.operation_id, second.dispatch_token)

    with psycopg.connect(ledger.dsn) as conn:
        for operation, age in ((first, 3), (second, 2), (third, 1)):
            conn.execute(
                sql.SQL(
                    """UPDATE {s}.operations
                    SET expires_at = clock_timestamp() - %s * interval '1 minute'
                    WHERE tenant_id=%s AND operation_id=%s"""
                ).format(s=sql.Identifier(ledger.schema)),
                (age, tenant, operation.operation_id),
            )
        conn.execute(
            sql.SQL("""CREATE FUNCTION {s}.fail_unknown_event()
            RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                IF NEW.kind = 'unknown' THEN
                    RAISE EXCEPTION 'injected recovery journal failure';
                END IF;
                RETURN NEW;
            END; $$;
            CREATE TRIGGER fail_unknown_event BEFORE INSERT ON {s}.events
            FOR EACH ROW EXECUTE FUNCTION {s}.fail_unknown_event();""").format(
                s=sql.Identifier(ledger.schema)
            )
        )

    with pytest.raises(psycopg.errors.RaiseException, match="injected recovery journal failure"):
        ledger.recover_expired(tenant)

    fresh = Ledger(ledger.dsn, schema=ledger.schema)
    assert fresh.get_operation(tenant, first.operation_id).status == Status.CANCELLED
    assert fresh.get_operation(tenant, second.operation_id).status == Status.DISPATCHED
    assert fresh.get_operation(tenant, third.operation_id).status == Status.RESERVED
    assert fresh.inspect_budget(tenant, "customer").held_micro == 60_000

    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL("DROP TRIGGER fail_unknown_event ON {}.events").format(
                sql.Identifier(ledger.schema)
            )
        )

    [recovered] = fresh.recover_expired(tenant, limit=1)
    assert (recovered.operation_id, recovered.status) == (second.operation_id, Status.UNKNOWN)
    assert fresh.inspect_budget(tenant, "customer").held_micro == 60_000

    [recovered] = fresh.recover_expired(tenant)
    assert (recovered.operation_id, recovered.status) == (third.operation_id, Status.CANCELLED)
    assert fresh.recover_expired(tenant) == []
    budget = fresh.inspect_budget(tenant, "customer")
    assert (budget.held_micro, budget.spent_micro) == (40_000, 0)
    for operation, expected in (
        (first, ["reserved", "expired_before_dispatch"]),
        (second, ["reserved", "dispatched", "unknown"]),
        (third, ["reserved", "expired_before_dispatch"]),
    ):
        assert [event["kind"] for event in fresh.events(tenant, operation.operation_id)] == expected
