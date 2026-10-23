import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from quotalatch import (
    BudgetExceeded,
    Conflict,
    DispatchDenied,
    InvalidTransition,
    NotFound,
    Status,
)

from .conftest import expire


def reserve(ledger, key="other", amount=30_000, ids=("customer",), **kwargs):
    return ledger.reserve(
        "tenant-a",
        key,
        ids,
        amount,
        request_fingerprint=f"request:{key}",
        **kwargs,
    )


def worker(ledger, mode, key):
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("process_worker.py")),
            mode,
            ledger.schema,
            key,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result


def test_reserve_settle_and_idempotent_retries(ledger, reserved):
    replay = ledger.reserve(
        "tenant-a",
        reserved.operation_key,
        ["customer"],
        300_000,
        request_fingerprint=reserved.request_fingerprint,
    )
    assert replay == reserved
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 300_000
    ledger.mark_dispatched("tenant-a", reserved.operation_id, reserved.dispatch_token)
    settled = ledger.settle(
        "tenant-a",
        reserved.operation_id,
        200_000,
        receipt_ref="receipt:1",
    )
    assert (
        ledger.settle(
            "tenant-a",
            reserved.operation_id,
            200_000,
            receipt_ref="receipt:1",
        )
        == settled
    )
    budget = ledger.inspect_budget("tenant-a", "customer")
    assert (budget.spent_micro, budget.held_micro, budget.available_micro) == (200_000, 0, 800_000)
    assert [e["kind"] for e in ledger.events("tenant-a", reserved.operation_id)] == [
        "reserved",
        "dispatched",
        "settled",
    ]


def test_multi_scope_admission_rolls_back_every_scope(ledger):
    ledger.create_budget("tenant-a", "customer", 1_000_000)
    ledger.create_budget("tenant-a", "job", 10_000)
    with pytest.raises(BudgetExceeded):
        reserve(ledger, ids=["customer", "job"])
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 0
    assert ledger.inspect_budget("tenant-a", "job").held_micro == 0
    with pytest.raises(NotFound):
        ledger.find_operation("tenant-a", "other")


def test_multiple_scopes_settle_in_full(ledger):
    for key in ("customer", "job"):
        ledger.create_budget("tenant-a", key, 100_000)
    op = reserve(ledger, ids=["job", "customer"])
    ledger.mark_dispatched("tenant-a", op.operation_id, op.dispatch_token)
    ledger.settle("tenant-a", op.operation_id, 20_000, receipt_ref="receipt:1")
    for key in ("customer", "job"):
        budget = ledger.inspect_budget("tenant-a", key)
        assert (budget.spent_micro, budget.held_micro) == (20_000, 0)


def test_100_concurrent_reservations_cannot_oversubscribe(ledger):
    ledger.create_budget("tenant-a", "customer", 1_000_000)

    def attempt(index):
        try:
            return reserve(ledger, str(index))
        except BudgetExceeded:
            return None

    with ThreadPoolExecutor(max_workers=24) as pool:
        admitted = [op for op in pool.map(attempt, range(100)) if op]
    assert len(admitted) == 33
    budget = ledger.inspect_budget("tenant-a", "customer")
    assert (budget.spent_micro, budget.held_micro) == (0, 990_000)


def test_independent_processes_share_one_budget(ledger):
    ledger.create_budget("tenant-a", "customer", 150_000)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda i: worker(ledger, "reserve", str(i)), range(24)))
    assert all(r.returncode == 0 for r in results), [r.stderr for r in results]
    assert sum(r.stdout.strip() != "denied" for r in results) == 5
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 150_000


def test_concurrent_same_key_reserves_once(ledger):
    ledger.create_budget("tenant-a", "customer", 100_000)
    with ThreadPoolExecutor(max_workers=12) as pool:
        ops = list(pool.map(lambda _: reserve(ledger, "same"), range(20)))
    assert len({op.operation_id for op in ops}) == 1
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 30_000
    assert len(ledger.events("tenant-a", ops[0].operation_id)) == 1


@pytest.mark.parametrize("change", ["amount", "fingerprint", "budgets", "ttl"])
def test_same_key_different_request_conflicts(ledger, reserved, change):
    ledger.create_budget("tenant-a", "job", 1_000_000)
    params = dict(
        tenant_id="tenant-a",
        operation_key=reserved.operation_key,
        budget_ids=["customer"],
        bound_micro=300_000,
        request_fingerprint=reserved.request_fingerprint,
    )
    if change == "amount":
        params["bound_micro"] = 1
    elif change == "fingerprint":
        params["request_fingerprint"] = "different-request"
    elif change == "budgets":
        params["budget_ids"] = ["customer", "job"]
    else:
        params["ttl_seconds"] = 1
    with pytest.raises(Conflict):
        ledger.reserve(**params)
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 300_000


def test_dispatch_claim_has_one_winner(ledger, reserved):
    def claim(_):
        try:
            ledger.mark_dispatched("tenant-a", reserved.operation_id, reserved.dispatch_token)
            return True
        except DispatchDenied:
            return False

    with ThreadPoolExecutor(max_workers=10) as pool:
        assert sum(pool.map(claim, range(20))) == 1


def test_wrong_dispatch_token_and_expiry_are_fenced(ledger, reserved):
    with pytest.raises(DispatchDenied):
        ledger.mark_dispatched("tenant-a", reserved.operation_id, uuid4())
    expire(ledger, reserved.operation_id)
    with pytest.raises(DispatchDenied):
        ledger.mark_dispatched("tenant-a", reserved.operation_id, reserved.dispatch_token)
    ledger.recover_expired("tenant-a")
    with pytest.raises(DispatchDenied):
        ledger.mark_dispatched("tenant-a", reserved.operation_id, reserved.dispatch_token)
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 0


def test_timeout_retains_hold_until_evidence(ledger, dispatched):
    ledger.mark_unknown("tenant-a", dispatched.operation_id, reason="provider timeout")
    budget = ledger.inspect_budget("tenant-a", "customer")
    assert (budget.spent_micro, budget.held_micro) == (0, 300_000)
    with pytest.raises(InvalidTransition):
        ledger.cancel_before_dispatch("tenant-a", dispatched.operation_id)
    with pytest.raises(InvalidTransition):
        ledger.settle("tenant-a", dispatched.operation_id, 0, receipt_ref="receipt:zero")
    ledger.reconcile("tenant-a", dispatched.operation_id, 0, receipt_ref="verified:no-charge")
    assert ledger.inspect_budget("tenant-a", "customer").available_micro == 1_000_000


def test_missing_cost_cannot_become_zero(ledger, dispatched):
    with pytest.raises(ValueError):
        ledger.settle("tenant-a", dispatched.operation_id, None, receipt_ref="receipt:unknown")
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 300_000
    ledger.mark_unknown("tenant-a", dispatched.operation_id, reason="usage missing")
    with pytest.raises(ValueError):
        ledger.reconcile("tenant-a", dispatched.operation_id, 0, receipt_ref="")


def test_actual_overage_is_kept_and_blocks_new_work(ledger, dispatched):
    ledger.settle("tenant-a", dispatched.operation_id, 1_200_000, receipt_ref="receipt:overage")
    budget = ledger.inspect_budget("tenant-a", "customer")
    assert (budget.spent_micro, budget.held_micro, budget.available_micro) == (
        1_200_000,
        0,
        -200_000,
    )
    assert budget.over_limit
    with pytest.raises(BudgetExceeded):
        reserve(ledger)
    assert ledger.events("tenant-a", dispatched.operation_id)[-1]["details"]["over_bound"]


def test_concurrent_settlement_is_idempotent(ledger, dispatched):
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(
            pool.map(
                lambda _: ledger.settle(
                    "tenant-a",
                    dispatched.operation_id,
                    200_000,
                    receipt_ref="receipt:1",
                ),
                range(20),
            )
        )
    assert all(op.status == Status.SETTLED for op in results)
    assert ledger.inspect_budget("tenant-a", "customer").spent_micro == 200_000
    assert len(ledger.events("tenant-a", dispatched.operation_id)) == 3
    for cost, receipt in [(100_000, "receipt:1"), (200_000, "different-receipt")]:
        with pytest.raises(Conflict):
            ledger.settle("tenant-a", dispatched.operation_id, cost, receipt_ref=receipt)


def test_concurrent_recovery_never_releases_dispatched_exposure(ledger, dispatched):
    expire(ledger, dispatched.operation_id)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: ledger.recover_expired("tenant-a"), range(12)))
    op = ledger.get_operation("tenant-a", dispatched.operation_id)
    assert op.status == Status.UNKNOWN
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 300_000
    assert len(ledger.events("tenant-a", dispatched.operation_id)) == 3


def test_recovery_races_settlement_without_losing_charge(ledger, dispatched):
    expire(ledger, dispatched.operation_id)

    def complete():
        try:
            return ledger.settle(
                "tenant-a", dispatched.operation_id, 200_000, receipt_ref="receipt:1"
            )
        except InvalidTransition:
            return ledger.reconcile(
                "tenant-a",
                dispatched.operation_id,
                200_000,
                receipt_ref="receipt:1",
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(ledger.recover_expired, "tenant-a")
        b = pool.submit(complete)
        a.result()
        b.result()
    budget = ledger.inspect_budget("tenant-a", "customer")
    assert (budget.spent_micro, budget.held_micro) == (200_000, 0)


@pytest.mark.parametrize(
    "mode,exit_code,expected",
    [
        ("crash-before-dispatch", 21, Status.CANCELLED),
        ("crash-after-dispatch", 22, Status.UNKNOWN),
        ("crash-after-settle", 23, Status.SETTLED),
    ],
)
def test_process_death_and_fresh_client_recovery(ledger, mode, exit_code, expected):
    ledger.create_budget("tenant-a", "customer", 1_000_000)
    result = worker(ledger, mode, "crashed")
    assert result.returncode == exit_code, result.stderr
    op = ledger.find_operation("tenant-a", "crashed")
    expire(ledger, op.operation_id)
    # New instance has no in-memory state from either the worker or the prior client.
    fresh = type(ledger)(ledger.dsn, schema=ledger.schema)
    fresh.recover_expired("tenant-a")
    op = fresh.get_operation("tenant-a", op.operation_id)
    assert op.status == expected
    if expected == Status.SETTLED:
        fresh.settle("tenant-a", op.operation_id, 20_000, receipt_ref="receipt:crashed")
        assert fresh.inspect_budget("tenant-a", "customer").spent_micro == 20_000
    else:
        assert fresh.inspect_budget("tenant-a", "customer").held_micro == (
            30_000 if expected == Status.UNKNOWN else 0
        )


def test_tenant_isolation(ledger, reserved):
    ledger.create_budget("tenant-b", "customer", 500_000)
    with pytest.raises(NotFound):
        ledger.get_operation("tenant-b", reserved.operation_id)
    with pytest.raises(NotFound):
        ledger.mark_dispatched("tenant-b", reserved.operation_id, reserved.dispatch_token)
    with pytest.raises(NotFound):
        ledger.settle("tenant-b", reserved.operation_id, 1, receipt_ref="receipt:1")
    with pytest.raises(NotFound):
        ledger.events("tenant-b", reserved.operation_id)
    assert ledger.recover_expired("tenant-b") == []
    assert ledger.inspect_budget("tenant-b", "customer").held_micro == 0


def test_reservation_and_job_enqueue_rollback_together(ledger):
    ledger.create_budget("tenant-a", "customer", 100_000)
    with psycopg.connect(ledger.dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("CREATE TABLE {}.jobs (operation_id uuid)").format(
                sql.Identifier(ledger.schema),
            )
        )
        with pytest.raises(RuntimeError), conn.transaction():
            op = reserve(ledger, connection=conn)
            conn.execute(
                sql.SQL("INSERT INTO {}.jobs VALUES (%s)").format(
                    sql.Identifier(ledger.schema),
                ),
                (op.operation_id,),
            )
            raise RuntimeError("enqueue failed")
        assert (
            conn.execute(
                sql.SQL("SELECT count(*) FROM {}.jobs").format(
                    sql.Identifier(ledger.schema),
                )
            ).fetchone()[0]
            == 0
        )
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 0
    with pytest.raises(NotFound):
        ledger.find_operation("tenant-a", "other")


def test_borrowed_connection_requires_outer_transaction(ledger):
    with psycopg.connect(ledger.dsn, autocommit=True) as conn, pytest.raises(ValueError):
        ledger.create_budget("tenant-a", "customer", 1, connection=conn)


def test_domain_error_savepoint_does_not_abort_outer_transaction(ledger):
    ledger.create_budget("tenant-a", "customer", 1)
    with psycopg.connect(ledger.dsn, autocommit=True) as conn, conn.transaction():
        with pytest.raises(BudgetExceeded):
            reserve(ledger, connection=conn)
        ledger.create_budget("tenant-a", "job", 2, connection=conn)
    assert ledger.inspect_budget("tenant-a", "job").limit_micro == 2


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE {s}.events SET kind='forged'",
        "DELETE FROM {s}.events",
        "TRUNCATE {s}.events",
    ],
)
def test_journal_is_append_only(ledger, reserved, statement):
    with psycopg.connect(ledger.dsn) as conn, pytest.raises(psycopg.errors.RaiseException):
        conn.execute(sql.SQL(statement).format(s=sql.Identifier(ledger.schema)))
    assert len(ledger.events("tenant-a", reserved.operation_id)) == 1


@pytest.mark.parametrize("amount", [-1, 0.1, float("nan"), float("inf"), True, 2**63])
def test_invalid_amounts_rejected_without_writing(ledger, amount):
    with pytest.raises(ValueError):
        ledger.create_budget("tenant-a", "customer", amount)


def test_cancelled_operation_key_cannot_reopen(ledger, reserved):
    ledger.cancel_before_dispatch("tenant-a", reserved.operation_id)
    ledger.cancel_before_dispatch("tenant-a", reserved.operation_id)
    replay = ledger.reserve(
        "tenant-a",
        reserved.operation_key,
        ["customer"],
        300_000,
        request_fingerprint=reserved.request_fingerprint,
    )
    assert replay.status == Status.CANCELLED
    with pytest.raises(DispatchDenied):
        ledger.mark_dispatched("tenant-a", replay.operation_id, replay.dispatch_token)


def test_migrate_is_repeatable_and_rejects_changed_version(ledger):
    ledger.migrate()
    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL("UPDATE {}.schema_version SET version=999").format(
                sql.Identifier(ledger.schema),
            )
        )
    with pytest.raises(Conflict):
        ledger.migrate()


def test_opposing_multi_scope_order_has_no_partial_admission(ledger):
    for budget_id in ("customer", "job"):
        ledger.create_budget("tenant-a", budget_id, 90_000)

    def attempt(index):
        ids = ["customer", "job"] if index % 2 else ["job", "customer"]
        try:
            return reserve(ledger, str(index), ids=ids)
        except BudgetExceeded:
            return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        admitted = [op for op in pool.map(attempt, range(24)) if op]
    assert len(admitted) == 3
    for budget_id in ("customer", "job"):
        assert ledger.inspect_budget("tenant-a", budget_id).held_micro == 90_000


def test_dispatch_racing_cancellation_cannot_release_paid_capacity(ledger, reserved):
    def dispatch():
        try:
            ledger.mark_dispatched("tenant-a", reserved.operation_id, reserved.dispatch_token)
            return True
        except DispatchDenied:
            return False

    def cancel():
        try:
            ledger.cancel_before_dispatch("tenant-a", reserved.operation_id)
            return True
        except InvalidTransition:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        a, b = pool.submit(dispatch), pool.submit(cancel)
        winners = (a.result(), b.result())
    assert sum(winners) == 1
    op = ledger.get_operation("tenant-a", reserved.operation_id)
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == (
        300_000 if op.status == Status.DISPATCHED else 0
    )


def test_database_event_failure_rolls_back_reservation(ledger):
    ledger.create_budget("tenant-a", "customer", 100_000)
    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL("""CREATE FUNCTION {s}.test_fail_insert()
            RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            RAISE EXCEPTION 'injected journal failure'; END; $$;
            CREATE TRIGGER test_fail_insert BEFORE INSERT ON {s}.events
            FOR EACH ROW EXECUTE FUNCTION {s}.test_fail_insert();""").format(
                s=sql.Identifier(ledger.schema)
            )
        )
    with pytest.raises(psycopg.errors.RaiseException, match="injected journal failure"):
        reserve(ledger)
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 0
    with pytest.raises(NotFound):
        ledger.find_operation("tenant-a", "other")


def test_settlement_write_failure_retains_hold_and_is_retryable(ledger, dispatched):
    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL("""CREATE FUNCTION {s}.test_fail_insert()
            RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            RAISE EXCEPTION 'injected settlement failure'; END; $$;
            CREATE TRIGGER test_fail_insert BEFORE INSERT ON {s}.events
            FOR EACH ROW EXECUTE FUNCTION {s}.test_fail_insert();""").format(
                s=sql.Identifier(ledger.schema)
            )
        )
    with pytest.raises(psycopg.errors.RaiseException, match="injected settlement failure"):
        ledger.settle("tenant-a", dispatched.operation_id, 200_000, receipt_ref="receipt:1")
    budget = ledger.inspect_budget("tenant-a", "customer")
    assert (budget.spent_micro, budget.held_micro) == (0, 300_000)
    assert ledger.get_operation("tenant-a", dispatched.operation_id).status == Status.DISPATCHED
    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL("DROP TRIGGER test_fail_insert ON {}.events").format(
                sql.Identifier(ledger.schema)
            )
        )
    ledger.settle("tenant-a", dispatched.operation_id, 200_000, receipt_ref="receipt:1")
    assert ledger.inspect_budget("tenant-a", "customer").spent_micro == 200_000


def test_uncommitted_dispatch_rollback_never_grants_durable_claim(ledger, reserved):
    with psycopg.connect(ledger.dsn, autocommit=True) as conn:
        with pytest.raises(RuntimeError), conn.transaction():
            ledger.mark_dispatched(
                "tenant-a",
                reserved.operation_id,
                reserved.dispatch_token,
                connection=conn,
            )
            raise RuntimeError("caller transaction failed; no provider call is allowed")
    assert ledger.get_operation("tenant-a", reserved.operation_id).status == Status.RESERVED
    ledger.mark_dispatched("tenant-a", reserved.operation_id, reserved.dispatch_token)


def test_late_reconciliation_after_repeated_recovery_is_idempotent(ledger, dispatched):
    expire(ledger, dispatched.operation_id)
    ledger.recover_expired("tenant-a")
    for _ in range(3):
        assert ledger.recover_expired("tenant-a") == []
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(
            pool.map(
                lambda _: ledger.reconcile(
                    "tenant-a",
                    dispatched.operation_id,
                    200_000,
                    receipt_ref="receipt:late",
                ),
                range(16),
            )
        )
    assert ledger.inspect_budget("tenant-a", "customer").spent_micro == 200_000
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 0


def test_exact_large_integer_accounting_avoids_float_rounding(ledger):
    amount = 2**53 + 1
    ledger.create_budget("tenant-a", "customer", amount)
    op = reserve(ledger, amount=amount)
    ledger.mark_dispatched("tenant-a", op.operation_id, op.dispatch_token)
    ledger.settle("tenant-a", op.operation_id, amount - 1, receipt_ref="receipt:large")
    assert ledger.inspect_budget("tenant-a", "customer").available_micro == 1


def test_process_death_inside_reservation_rolls_back_all_rows(ledger):
    ledger.create_budget("tenant-a", "customer", 100_000)
    result = worker(ledger, "crash-in-reservation", "crash-in-tx")
    assert result.returncode == 24, result.stderr
    assert ledger.inspect_budget("tenant-a", "customer").held_micro == 0
    with pytest.raises(NotFound):
        ledger.find_operation("tenant-a", "crash-in-tx")


def test_process_death_inside_settlement_retains_full_exposure(ledger):
    ledger.create_budget("tenant-a", "customer", 100_000)
    result = worker(ledger, "crash-in-settlement", "settlement-crash")
    assert result.returncode == 25, result.stderr
    op = ledger.find_operation("tenant-a", "settlement-crash")
    assert op.status == Status.DISPATCHED
    budget = ledger.inspect_budget("tenant-a", "customer")
    assert (budget.spent_micro, budget.held_micro) == (0, 30_000)
    ledger.settle("tenant-a", op.operation_id, 20_000, receipt_ref="receipt:settlement-crash")
    assert ledger.inspect_budget("tenant-a", "customer").spent_micro == 20_000
