# QuotaLatch

An experimental Python library for persistent, shared AI worker budgets in PostgreSQL.
Version 0.1.0a1 implements the ledger, integration tests, and a crash-and-retry demo.
All amounts are **integer micro-USD** (1 USD = 1,000,000 units).

This is an alpha release; APIs and the database schema may change. Download the
wheel or source archive from [GitHub releases](https://github.com/valkiara/quotalatch/releases).
Report bugs and propose improvements in [GitHub issues](https://github.com/valkiara/quotalatch/issues).

## Run locally

Requires Python 3.11+, uv, and Docker Compose (or an existing PostgreSQL 16+ database).

```sh
docker compose up -d --wait
uv sync --extra dev
```

PowerShell:

```powershell
$env:BUDGET_DATABASE_URL = 'postgresql://budget:budget-local-only@127.0.0.1:55439/quotalatch'
uv run pytest
uv run quotalatch-demo
```

POSIX shell:

```sh
export BUDGET_DATABASE_URL='postgresql://budget:budget-local-only@127.0.0.1:55439/quotalatch'
uv run pytest
uv run quotalatch-demo
```

Tests require a real PostgreSQL database and fail clearly when the URL is missing.
Each test gets a random schema and removes only that schema afterward. The demo
uses a unique tenant and a simulated provider with independent committed receipts.
It makes no model API calls and incurs no provider charges.

Stop the local database with `docker compose stop`. The Compose password is for
this localhost-only development database. Use managed secrets for deployments.

## Basic API

```python
import os
from quotalatch import Ledger

ledger = Ledger(os.environ["BUDGET_DATABASE_URL"])
ledger.migrate()  # run at deployment, before starting workers
ledger.create_budget("acme", "customer", 10_000_000)
ledger.create_budget("acme", "job-42", 1_000_000)

operation = ledger.reserve(
    "acme",
    "job-42:search:attempt-1",
    ["customer", "job-42"],
    30_000,
    request_fingerprint="sha256-of-canonical-request",
)
ledger.mark_dispatched("acme", operation.operation_id, operation.dispatch_token)
# Call the provider only AFTER the dispatch transaction has committed.
# On success, retain a receipt durably, then:
ledger.settle("acme", operation.operation_id, 20_000, receipt_ref="provider:receipt-123")
print(ledger.inspect_budget("acme", "job-42"))
```

`reserve` returns the existing operation on an identical retry, including its
current status. The dispatch claim succeeds **once**, not once per retry. A
lost dispatch response is ambiguous: do not retry the external operation blindly.
Use the provider's own idempotency and receipt lookup where available.

## Guarantees and boundary

- One PostgreSQL transaction reserves all applicable scopes or none. Budget rows
  lock in deterministic order. Every worker must use the same authoritative database.
- Known actual costs are recorded in full, even above the declared bound. An
  overdrawn budget reports negative availability and rejects further reservations.
- `settle` is idempotent for the same amount and receipt. A different terminal
  result or reused operation key with different parameters raises `Conflict`.
- Dispatched operations with unknown outcomes keep their full hold. A timeout,
  exception, cancellation, or expired lease never proves that a call was free.
- Run `recover_expired(tenant_id)` from your application scheduler. It cancels
  expired undispatched reservations, fencing stale dispatch claims, and marks
  expired dispatched operations unknown. It never automatically refunds unknown work.
- Use `mark_unknown(..., reason=...)` after ambiguous provider outcomes, and
  `reconcile(..., actual_micro=..., receipt_ref=...)` after verifying external evidence.
  Zero cost requires explicit evidence too. The library cannot authenticate provider receipts.
- The journal rejects UPDATE, DELETE, and TRUNCATE through a database trigger.
  It is not tamper-proof against the database owner or an administrator.
- `tenant_id` scopes every query. It is a **trusted application boundary**, not
  end-user authentication or PostgreSQL row-level security. Derive it from your
  authenticated server context. Clients holding database credentials are trusted.
- This controls admission against declared bounds. It does not guarantee a
  provider invoice cap when actual usage exceeds estimates, prices change, callers
  bypass the library, or a provider performs unaccounted retries. Use a separate
  operation key for each genuinely new billable attempt. Ledger idempotency is not
  external exactly-once execution.
- Money inputs accept nonnegative integers up to 2^63-1. Balances use PostgreSQL
  NUMERIC(38,0) to retain aggregate overages without floating-point arithmetic.

## Lifecycle

```text
reserved -> dispatched -> settled
    |           |
    v           v
cancelled     unknown -> settled (reconcile with evidence)
```

Cancelling is valid only before dispatch. Unknown holds remain until explicitly
reconciled; this deliberately trades availability for conservative accounting.
Expired or cancelled keys cannot be reused to create another attempt.

## Share a transaction with your job queue

Mutations accept `connection=conn` on a borrowed psycopg connection already inside
an explicit transaction. Methods use savepoints and never commit the outer transaction.

```python
import psycopg

with psycopg.connect(ledger.dsn, autocommit=True) as conn:
    with conn.transaction():
        operation = ledger.reserve(
            "acme",
            "job-43:attempt-1",
            ["customer"],
            40_000,
            request_fingerprint="request-digest",
            connection=conn,
        )
        # Insert your job/outbox row using conn in this same transaction.
    # Only now is work eligible for dispatch.
```

Keep external calls outside database transactions. Database failures propagate;
no automatic transaction retries or external calls happen inside the library.
PostgreSQL deadlocks/serialization failures require retrying the entire application
transaction, using the same operation key. Batch application transactions should
acquire their own resources in a consistent order too.

## Development

```sh
uv run ruff check .
uv run ruff format --check .
uv build
```

CI is configured to run tests against PostgreSQL 16 and 17, including independent worker processes,
crashes, concurrent settlement/recovery, tenant isolation, and queue rollback.
Sync psycopg connections keep this first release small; async adapters, model pricing,
and provider-specific integrations are future work. Never put prompts, credentials,
or personal data in receipt references or reason strings.
