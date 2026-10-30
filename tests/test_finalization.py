"""Failures midway through finalization must preserve every budget's exposure."""

import psycopg
import pytest
from psycopg import sql

from quotalatch import Status

from .conftest import expire


@pytest.mark.parametrize(
    "action,borrowed",
    [
        ("settle", False),
        ("settle", True),
        ("reconcile", False),
        ("reconcile", True),
        ("cancel", False),
        ("cancel", True),
        ("recover", False),
    ],
)
def test_partial_finalization_failure_rolls_back_all_budgets(ledger, action, borrowed):
    for budget_id in ("customer", "job"):
        ledger.create_budget("tenant-a", budget_id, 1_000_000)
    operation = ledger.reserve(
        "tenant-a",
        "multi-budget-attempt",
        ["customer", "job"],
        300_000,
        request_fingerprint="request:multi-budget",
    )
    if action in ("settle", "reconcile"):
        ledger.mark_dispatched("tenant-a", operation.operation_id, operation.dispatch_token)
    if action == "reconcile":
        ledger.mark_unknown("tenant-a", operation.operation_id, reason="provider timeout")
    if action == "recover":
        expire(ledger, operation.operation_id)

    before_operation = ledger.get_operation("tenant-a", operation.operation_id)
    before_events = ledger.events("tenant-a", operation.operation_id)
    before_budgets = [ledger.inspect_budget("tenant-a", key) for key in ("customer", "job")]

    # Fail on the second budget after the first budget's balance has been updated.
    # A failure here must roll back the earlier write as well as the operation/journal.
    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL("""CREATE FUNCTION {s}.fail_job_finalization()
            RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                IF NEW.budget_id = 'job' AND NEW.held_micro < OLD.held_micro THEN
                    RAISE EXCEPTION 'injected second-budget failure';
                END IF;
                RETURN NEW;
            END; $$;
            CREATE TRIGGER fail_job_finalization BEFORE UPDATE ON {s}.budgets
            FOR EACH ROW EXECUTE FUNCTION {s}.fail_job_finalization();""").format(
                s=sql.Identifier(ledger.schema)
            )
        )

    def finalize(connection=None):
        kwargs = {"connection": connection} if connection is not None else {}
        if action in ("settle", "reconcile"):
            return getattr(ledger, action)(
                "tenant-a",
                operation.operation_id,
                200_000,
                receipt_ref="receipt:multi-budget",
                **kwargs,
            )
        if action == "cancel":
            return ledger.cancel_before_dispatch("tenant-a", operation.operation_id, **kwargs)
        return ledger.recover_expired("tenant-a")

    with psycopg.connect(ledger.dsn, autocommit=True) as conn, conn.transaction():
        if borrowed:
            ledger.create_budget("tenant-a", "outer-before", 1, connection=conn)
        with pytest.raises(psycopg.errors.RaiseException, match="injected second-budget failure"):
            finalize(conn if borrowed else None)
        if borrowed:
            ledger.create_budget("tenant-a", "outer-after", 1, connection=conn)

    assert ledger.get_operation("tenant-a", operation.operation_id) == before_operation
    assert ledger.events("tenant-a", operation.operation_id) == before_events
    assert [ledger.inspect_budget("tenant-a", key) for key in ("customer", "job")] == before_budgets
    if borrowed:
        # The failed savepoint must neither abort nor commit the caller's transaction.
        for key in ("outer-before", "outer-after"):
            assert ledger.inspect_budget("tenant-a", key).limit_micro == 1

    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL("DROP TRIGGER fail_job_finalization ON {}.budgets").format(
                sql.Identifier(ledger.schema)
            )
        )

    finalize()
    finalize()  # A lost success response must also be safe to retry.
    charged = action in ("settle", "reconcile")
    final_operation = ledger.get_operation("tenant-a", operation.operation_id)
    assert final_operation.status == (Status.SETTLED if charged else Status.CANCELLED)
    for key in ("customer", "job"):
        budget = ledger.inspect_budget("tenant-a", key)
        assert (budget.spent_micro, budget.held_micro, budget.available_micro) == (
            200_000 if charged else 0,
            0,
            800_000 if charged else 1_000_000,
        )
    assert [event["kind"] for event in ledger.events("tenant-a", operation.operation_id)] == [
        *[event["kind"] for event in before_events],
        {"settle": "settled", "reconcile": "reconciled", "cancel": "cancelled"}.get(
            action, "expired_before_dispatch"
        ),
    ]
