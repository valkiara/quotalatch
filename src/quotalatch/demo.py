"""Crash real worker processes against a simulated, independently committing provider."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict
from uuid import uuid4

import psycopg
from psycopg import sql

from . import DispatchDenied, Ledger, Status


def _provider_call(ledger: Ledger, tenant: str, operation_id, amount: int) -> str:
    """Simulated provider: its receipt survives worker death independently of ledger settlement."""
    receipt = f"simulated:{uuid4()}"
    with psycopg.connect(ledger.dsn) as conn:
        conn.execute(
            sql.SQL("INSERT INTO {}.demo_provider_receipts VALUES (%s,%s,%s,%s)").format(
                sql.Identifier(ledger.schema)
            ),
            (receipt, tenant, operation_id, amount),
        )
    return receipt


def _worker(ledger: Ledger, tenant: str, stage: str) -> None:
    amount = {"before-dispatch": 400_000, "after-dispatch": 300_000, "after-settle": 250_000}[stage]
    op = ledger.reserve(
        tenant,
        stage,
        ["job"],
        amount,
        request_fingerprint=f"demo-request:{stage}",
        ttl_seconds=300 if stage == "after-settle" else 1,
    )
    if stage == "before-dispatch":
        os._exit(21)
    ledger.mark_dispatched(tenant, op.operation_id, op.dispatch_token)
    actual = 200_000 if stage == "after-dispatch" else 100_000
    receipt = _provider_call(ledger, tenant, op.operation_id, actual)
    if stage == "after-dispatch":
        os._exit(22)
    ledger.settle(tenant, op.operation_id, actual, receipt_ref=receipt)
    # Settlement committed, but the caller never receives a success response.
    os._exit(23)


def _crash(ledger: Ledger, tenant: str, stage: str, expected: int) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "quotalatch.demo",
            "--worker",
            stage,
            "--schema",
            ledger.schema,
            "--tenant",
            tenant,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != expected:
        raise RuntimeError(f"Worker exit {result.returncode}, expected {expected}: {result.stderr}")


def _recover(ledger: Ledger, tenant: str, key: str):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        ledger.recover_expired(tenant)
        operation = ledger.find_operation(tenant, key)
        if operation.status in (Status.CANCELLED, Status.UNKNOWN):
            return operation
        time.sleep(0.05)
    raise RuntimeError("Recovery deadline exceeded")


def run_demo(dsn: str, *, schema: str = "quotalatch_demo") -> dict:
    tenant = f"demo-{uuid4().hex}"
    ledger = Ledger(dsn, schema=schema)
    ledger.migrate()
    with psycopg.connect(dsn) as conn:
        conn.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {}.demo_provider_receipts (
                receipt_ref text PRIMARY KEY, tenant_id text NOT NULL,
                operation_id uuid NOT NULL, actual_micro bigint NOT NULL)""").format(
                sql.Identifier(schema)
            )
        )
    ledger.create_budget(tenant, "job", 1_000_000)

    _crash(ledger, tenant, "before-dispatch", 21)
    fresh = Ledger(dsn, schema=schema)
    before = _recover(fresh, tenant, "before-dispatch")
    assert before.status == Status.CANCELLED
    assert fresh.inspect_budget(tenant, "job").held_micro == 0
    print("PASS: crash before dispatch -> hold released; stale worker fenced", flush=True)
    try:
        fresh.mark_dispatched(tenant, before.operation_id, before.dispatch_token)
    except DispatchDenied:
        pass
    else:
        raise AssertionError("Expired worker unexpectedly won dispatch")

    _crash(ledger, tenant, "after-dispatch", 22)
    unknown = _recover(fresh, tenant, "after-dispatch")
    assert unknown.status == Status.UNKNOWN
    assert fresh.inspect_budget(tenant, "job").held_micro == 300_000
    retry = fresh.reserve(
        tenant,
        "after-dispatch",
        ["job"],
        300_000,
        request_fingerprint="demo-request:after-dispatch",
        ttl_seconds=1,
    )
    try:
        fresh.mark_dispatched(tenant, retry.operation_id, retry.dispatch_token)
    except DispatchDenied:
        pass
    else:
        raise AssertionError("Retried job unexpectedly dispatched again")
    print("PASS: crash after paid effect -> $0.30 held; retry cannot dispatch", flush=True)

    with psycopg.connect(dsn) as conn:
        receipt, actual = conn.execute(
            sql.SQL(
                "SELECT receipt_ref, actual_micro FROM {}.demo_provider_receipts "
                "WHERE tenant_id=%s AND operation_id=%s"
            ).format(sql.Identifier(schema)),
            (tenant, unknown.operation_id),
        ).fetchone()
    fresh.reconcile(tenant, unknown.operation_id, actual, receipt_ref=receipt)
    assert fresh.inspect_budget(tenant, "job").spent_micro == 200_000
    print("PASS: independent provider receipt -> $0.20 settled, $0.10 released", flush=True)

    _crash(ledger, tenant, "after-settle", 23)
    settled = fresh.find_operation(tenant, "after-settle")
    fresh.settle(tenant, settled.operation_id, 100_000, receipt_ref=settled.receipt_ref)
    final = fresh.inspect_budget(tenant, "job")
    with psycopg.connect(dsn) as conn:
        provider_calls = conn.execute(
            sql.SQL("SELECT count(*) FROM {}.demo_provider_receipts WHERE tenant_id=%s").format(
                sql.Identifier(schema)
            ),
            (tenant,),
        ).fetchone()[0]
    assert (final.spent_micro, final.held_micro, provider_calls) == (300_000, 0, 2)
    print("PASS: lost settlement response -> retry does not double charge", flush=True)
    result = {"tenant_id": tenant, "provider_calls": provider_calls, "budget": asdict(final)}
    print(json.dumps(result, indent=2), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="quotalatch_demo")
    parser.add_argument("--tenant")
    parser.add_argument("--worker", choices=["before-dispatch", "after-dispatch", "after-settle"])
    args = parser.parse_args()
    dsn = os.environ.get("BUDGET_DATABASE_URL")
    if not dsn:
        parser.error("Set BUDGET_DATABASE_URL to a PostgreSQL database; see README.md")
    if args.worker:
        if not args.tenant:
            parser.error("--tenant is required in worker mode")
        _worker(Ledger(dsn, schema=args.schema), args.tenant, args.worker)
    else:
        run_demo(dsn, schema=args.schema)


if __name__ == "__main__":
    main()
