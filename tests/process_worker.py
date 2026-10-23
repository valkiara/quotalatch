"""Independent process used for actual crash and cross-process concurrency tests."""

import os
import sys

import psycopg

from quotalatch import BudgetExceeded, Ledger

mode, schema, key = sys.argv[1:]
ledger = Ledger(os.environ["BUDGET_DATABASE_URL"], schema=schema)
op = None
if mode == "crash-in-reservation":
    with psycopg.connect(ledger.dsn, autocommit=True) as conn, conn.transaction():
        ledger.reserve(
            "tenant-a",
            key,
            ["customer"],
            30_000,
            request_fingerprint=f"request:{key}",
            connection=conn,
        )
        os._exit(24)
try:
    op = ledger.reserve(
        "tenant-a",
        key,
        ["customer"],
        30_000,
        request_fingerprint=f"request:{key}",
    )
except BudgetExceeded:
    print("denied", flush=True)
    sys.exit(0)
if mode == "reserve":
    print(str(op.operation_id), flush=True)
    sys.exit(0)
if mode == "crash-before-dispatch":
    os._exit(21)
ledger.mark_dispatched("tenant-a", op.operation_id, op.dispatch_token)
if mode == "crash-in-settlement":
    with psycopg.connect(ledger.dsn, autocommit=True) as conn, conn.transaction():
        ledger.settle(
            "tenant-a",
            op.operation_id,
            20_000,
            receipt_ref=f"receipt:{key}",
            connection=conn,
        )
        os._exit(25)
if mode == "crash-after-dispatch":
    os._exit(22)
if mode == "crash-after-settle":
    ledger.settle("tenant-a", op.operation_id, 20_000, receipt_ref=f"receipt:{key}")
    os._exit(23)
raise ValueError(mode)
