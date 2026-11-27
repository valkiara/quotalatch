# Contributing to QuotaLatch

Bug reports, failure tests, documentation improvements, and focused fixes are
welcome. Check the [open issues](https://github.com/valkiara/quotalatch/issues)
and [pull requests](https://github.com/valkiara/quotalatch/pulls) before starting
to avoid duplicating an existing contribution. For a larger API or schema change,
describe the intended behavior in an issue first.

## Prepare a development checkout

Fork the repository, clone your fork, and enter its root directory. You need
Python 3.11+, uv, and a disposable PostgreSQL 16+ database. The supplied
[Compose configuration](compose.yaml) provides PostgreSQL on localhost:

```sh
uv sync --locked --extra dev
docker compose up -d --wait
```

Set `BUDGET_DATABASE_URL` in the same shell that runs the tests. The
[README](README.md#run-locally) includes PowerShell and POSIX examples. The test
database user must be able to create schemas, tables, functions, and triggers,
and drop its test schemas. Use a development database rather than production.

The tests use real PostgreSQL transactions and independent worker processes.
Each ledger fixture creates a random schema and drops that schema afterward.
Missing database configuration fails the suite; it does not skip the integration
tests. The demo uses a simulated provider and needs no model API key.

## Find the relevant code

| Area | Source |
| --- | --- |
| Transactions, reservations, dispatch, settlement, and recovery | [ledger.py](src/quotalatch/ledger.py) |
| Public value objects and operation states | [models.py](src/quotalatch/models.py) |
| Domain exceptions | [errors.py](src/quotalatch/errors.py) |
| Versioned database schema | [schema.sql](src/quotalatch/schema.sql) |
| Crash-and-retry demo | [demo.py](src/quotalatch/demo.py) |
| Integration fixtures and failure tests | [tests](tests/) |
| Automated checks | [CI workflow](.github/workflows/ci.yml) |

For ledger fixes, show the failing behavior with a focused test. Assert the
resulting balances, operation state, and journal when the change affects
accounting. Prefer controlled failure injection or synchronization over timing
assumptions in concurrency tests.

## Preserve the ledger contract

Read the [guarantees and boundaries](README.md#guarantees-and-boundary) before
changing lifecycle behavior. In particular, an ambiguous dispatched outcome keeps
its hold until verified usage can be recorded. A retry of an operation key must
not create a second reservation or authorize another provider call.

`schema.sql` is checksummed by `Ledger.migrate()`. Editing that published
migration makes an existing database reject the changed checksum. Schema changes
need a migration strategy that preserves existing version/checksum records and
tests an upgrade from the prior schema. Do not work around a mismatch by deleting
or rewriting a deployed database's migration marker.

Borrowed connections belong to an application transaction: ledger methods must
not commit the caller's work. Keep provider calls outside database transactions.

## Validate and open a pull request

From the repository root, with the database URL configured:

```sh
uv run pytest
uv run quotalatch-demo
uv run ruff check .
uv run ruff format --check .
uv build
```

You can use `uv run pytest tests/test_ledger.py -k recovery` to iterate on a
focused area, then run the relevant full checks before submitting. Stop your
local database with `docker compose stop` when finished.

Open one focused PR describing the triggering problem, the resulting behavior,
and how you verified it. Explain any schema compatibility impact. If a check
could not run, state which one and why. Do not include credentials, prompts,
personal data, local environments, or generated distributions in the change.
