"""Synchronous PostgreSQL ledger. Network/provider calls belong outside its transactions."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from importlib.resources import files
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg import Connection, sql
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .errors import BudgetExceeded, Conflict, DispatchDenied, InvalidTransition, NotFound
from .models import Budget, Operation, Status

MAX_AMOUNT = 2**63 - 1


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 1024 or "\x00" in value:
        raise ValueError(f"{name} must be nonempty text of at most 1024 characters")
    return value


def _amount(value: int) -> int:
    if type(value) is not int or not 0 <= value <= MAX_AMOUNT:
        raise ValueError(f"Amount must be an integer between 0 and {MAX_AMOUNT} micro-USD")
    return value


class Ledger:
    def __init__(self, dsn: str, *, schema: str = "quotalatch"):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
            raise ValueError("schema must be a lowercase PostgreSQL identifier")
        self.dsn = dsn
        self.schema = schema

    def _sql(self, statement: str) -> sql.Composed:
        return sql.SQL(statement).format(s=sql.Identifier(self.schema))

    @contextmanager
    def _tx(self, connection: Connection | None = None) -> Iterator[Connection]:
        if connection is not None:
            if connection.info.transaction_status != TransactionStatus.INTRANS:
                raise ValueError("A borrowed connection must already be in an explicit transaction")
            # A savepoint makes domain errors rollback-safe inside the caller's transaction.
            with connection.transaction():
                yield connection
        else:
            with psycopg.connect(self.dsn, autocommit=True) as conn, conn.transaction():
                yield conn

    def _one(self, conn: Connection, statement: str, params: Any = ()) -> dict | None:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(self._sql(statement), params)
            return cursor.fetchone()

    def _all(self, conn: Connection, statement: str, params: Any = ()) -> list[dict]:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(self._sql(statement), params)
            return cursor.fetchall()

    def _execute(self, conn: Connection, statement: str, params: Any = ()) -> None:
        conn.execute(self._sql(statement), params)

    def migrate(self) -> None:
        """Apply schema v1 atomically; reject unknown versions or changed migration contents."""
        source = files("quotalatch").joinpath("schema.sql").read_text(encoding="utf-8")
        checksum = hashlib.sha256(source.encode()).hexdigest()
        with self._tx() as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"quotalatch:migration:{self.schema}",),
            )
            self._execute(conn, "CREATE SCHEMA IF NOT EXISTS {s}")
            self._execute(
                conn,
                """CREATE TABLE IF NOT EXISTS {s}.schema_version (
                    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                    version integer NOT NULL, checksum text NOT NULL)""",
            )
            version = self._one(conn, "SELECT version, checksum FROM {s}.schema_version")
            if version:
                if version != {"version": 1, "checksum": checksum}:
                    raise Conflict("Unsupported schema version or changed migration checksum")
                return
            conn.execute(sql.SQL(source).format(schema=sql.Identifier(self.schema)))
            self._execute(
                conn,
                "INSERT INTO {s}.schema_version (version, checksum) VALUES (1, %s)",
                (checksum,),
            )

    @staticmethod
    def _budget(row: dict) -> Budget:
        return Budget(
            row["tenant_id"],
            row["budget_id"],
            int(row["limit_micro"]),
            int(row["spent_micro"]),
            int(row["held_micro"]),
        )

    @staticmethod
    def _operation(row: dict) -> Operation:
        return Operation(
            tenant_id=row["tenant_id"],
            operation_id=row["operation_id"],
            operation_key=row["operation_key"],
            request_fingerprint=row["request_fingerprint"],
            status=Status(row["status"]),
            bound_micro=int(row["bound_micro"]),
            actual_micro=int(row["actual_micro"]) if row["actual_micro"] is not None else None,
            dispatch_token=row["dispatch_token"],
            expires_at=row["expires_at"],
            receipt_ref=row["receipt_ref"],
            unknown_reason=row["unknown_reason"],
        )

    def create_budget(
        self,
        tenant_id: str,
        budget_id: str,
        limit_micro: int,
        *,
        connection: Connection | None = None,
    ) -> Budget:
        _text(tenant_id, "tenant_id")
        _text(budget_id, "budget_id")
        _amount(limit_micro)
        with self._tx(connection) as conn:
            self._execute(
                conn,
                """INSERT INTO {s}.budgets (tenant_id, budget_id, limit_micro)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
                (tenant_id, budget_id, limit_micro),
            )
            row = self._one(
                conn,
                "SELECT * FROM {s}.budgets WHERE tenant_id=%s AND budget_id=%s",
                (tenant_id, budget_id),
            )
            if row["limit_micro"] != limit_micro:
                raise Conflict("Budget already exists with a different limit")
            return self._budget(row)

    def inspect_budget(
        self,
        tenant_id: str,
        budget_id: str,
        *,
        connection: Connection | None = None,
    ) -> Budget:
        with self._tx(connection) as conn:
            row = self._one(
                conn,
                "SELECT * FROM {s}.budgets WHERE tenant_id=%s AND budget_id=%s",
                (tenant_id, budget_id),
            )
            if row is None:
                raise NotFound("Budget not found")
            return self._budget(row)

    def reserve(
        self,
        tenant_id: str,
        operation_key: str,
        budget_ids: Sequence[str],
        bound_micro: int,
        *,
        request_fingerprint: str,
        ttl_seconds: int = 300,
        connection: Connection | None = None,
    ) -> Operation:
        _text(tenant_id, "tenant_id")
        _text(operation_key, "operation_key")
        _text(request_fingerprint, "request_fingerprint")
        _amount(bound_micro)
        if isinstance(budget_ids, str) or not 1 <= len(budget_ids) <= 32:
            raise ValueError("Supply between 1 and 32 budget IDs")
        ids = sorted({_text(b, "budget_id") for b in budget_ids})
        if len(ids) != len(budget_ids):
            raise ValueError("Duplicate budget IDs")
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 604800:
            raise ValueError("ttl_seconds must be an integer between 1 and 604800")
        request_hash = hashlib.sha256(
            json.dumps(
                [ids, bound_micro, request_fingerprint, ttl_seconds],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with self._tx(connection) as conn:
            # Serialize retries of one key even before an operation row exists.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (json.dumps([self.schema, tenant_id, operation_key]),),
            )
            old = self._one(
                conn,
                "SELECT * FROM {s}.operations WHERE tenant_id=%s AND operation_key=%s",
                (tenant_id, operation_key),
            )
            if old:
                if old["request_hash"] != request_hash:
                    raise Conflict("Operation key was already bound to different parameters")
                return self._operation(old)
            budgets = self._all(
                conn,
                """SELECT * FROM {s}.budgets WHERE tenant_id=%s AND budget_id=ANY(%s)
                ORDER BY budget_id FOR UPDATE""",
                (tenant_id, ids),
            )
            if len(budgets) != len(ids):
                raise NotFound("One or more budgets not found")
            for row in budgets:
                budget = self._budget(row)
                if budget.available_micro < bound_micro:
                    raise BudgetExceeded(budget.budget_id, bound_micro, budget.available_micro)
            operation_id, token = uuid4(), uuid4()
            row = self._one(
                conn,
                """INSERT INTO {s}.operations (
                tenant_id, operation_id, operation_key, request_fingerprint, request_hash,
                status, bound_micro, dispatch_token, expires_at)
                VALUES (%s,%s,%s,%s,%s,'reserved',%s,%s,
                        clock_timestamp() + %s * interval '1 second') RETURNING *""",
                (
                    tenant_id,
                    operation_id,
                    operation_key,
                    request_fingerprint,
                    request_hash,
                    bound_micro,
                    token,
                    ttl_seconds,
                ),
            )
            for budget_id in ids:
                self._execute(
                    conn,
                    "INSERT INTO {s}.operation_budgets VALUES (%s,%s,%s)",
                    (tenant_id, operation_id, budget_id),
                )
                self._execute(
                    conn,
                    """UPDATE {s}.budgets SET held_micro=held_micro+%s
                    WHERE tenant_id=%s AND budget_id=%s""",
                    (bound_micro, tenant_id, budget_id),
                )
            self._event(conn, row, "reserved", {"bound_micro": bound_micro, "budgets": ids})
            return self._operation(row)

    def get_operation(
        self,
        tenant_id: str,
        operation_id: UUID,
        *,
        connection: Connection | None = None,
    ) -> Operation:
        with self._tx(connection) as conn:
            return self._operation(self._get(conn, tenant_id, operation_id))

    def find_operation(self, tenant_id: str, operation_key: str) -> Operation:
        with self._tx() as conn:
            row = self._one(
                conn,
                "SELECT * FROM {s}.operations WHERE tenant_id=%s AND operation_key=%s",
                (tenant_id, operation_key),
            )
            if not row:
                raise NotFound("Operation not found")
            return self._operation(row)

    def _get(self, conn: Connection, tenant: str, operation: UUID, *, lock=False) -> dict:
        row = self._one(
            conn,
            "SELECT * FROM {s}.operations WHERE tenant_id=%s AND operation_id=%s"
            + (" FOR UPDATE" if lock else ""),
            (tenant, operation),
        )
        if row is None:
            raise NotFound("Operation not found")
        return row

    def _event(self, conn: Connection, row: dict, kind: str, details: dict) -> None:
        self._execute(
            conn,
            """INSERT INTO {s}.events (tenant_id, operation_id, kind, details)
            VALUES (%s,%s,%s,%s)""",
            (row["tenant_id"], row["operation_id"], kind, Jsonb(details)),
        )

    def _set_state(self, conn: Connection, row: dict, state: str, reason=None) -> dict:
        return self._one(
            conn,
            """UPDATE {s}.operations SET status=%s, unknown_reason=%s
            WHERE tenant_id=%s AND operation_id=%s RETURNING *""",
            (state, reason, row["tenant_id"], row["operation_id"]),
        )

    def mark_dispatched(
        self,
        tenant_id: str,
        operation_id: UUID,
        dispatch_token: UUID,
        *,
        connection: Connection | None = None,
    ) -> Operation:
        """Claim dispatch ONCE. Do not call a provider until this transaction commits."""
        with self._tx(connection) as conn:
            row = self._get(conn, tenant_id, operation_id, lock=True)
            expired = self._one(
                conn,
                "SELECT clock_timestamp() >= %s AS expired",
                (row["expires_at"],),
            )["expired"]
            if (
                row["status"] != Status.RESERVED
                or row["dispatch_token"] != dispatch_token
                or expired
            ):
                raise DispatchDenied("Dispatch already claimed, expired, or token does not match")
            row = self._set_state(conn, row, Status.DISPATCHED)
            self._event(conn, row, "dispatched", {})
            return self._operation(row)

    def _finish_balances(self, conn: Connection, row: dict, actual: int) -> None:
        budgets = self._all(
            conn,
            """SELECT b.budget_id FROM {s}.budgets b JOIN {s}.operation_budgets ob
            ON (b.tenant_id,b.budget_id)=(ob.tenant_id,ob.budget_id)
            WHERE ob.tenant_id=%s AND ob.operation_id=%s ORDER BY b.budget_id FOR UPDATE OF b""",
            (row["tenant_id"], row["operation_id"]),
        )
        for budget in budgets:
            self._execute(
                conn,
                """UPDATE {s}.budgets SET held_micro=held_micro-%s, spent_micro=spent_micro+%s
                WHERE tenant_id=%s AND budget_id=%s""",
                (row["bound_micro"], actual, row["tenant_id"], budget["budget_id"]),
            )

    def settle(
        self,
        tenant_id: str,
        operation_id: UUID,
        actual_micro: int,
        *,
        receipt_ref: str,
        connection: Connection | None = None,
    ) -> Operation:
        return self._settle(tenant_id, operation_id, actual_micro, receipt_ref, False, connection)

    def reconcile(
        self,
        tenant_id: str,
        operation_id: UUID,
        actual_micro: int,
        *,
        receipt_ref: str,
        connection: Connection | None = None,
    ) -> Operation:
        """Resolve an UNKNOWN outcome using a caller-verified receipt, including proven zero."""
        return self._settle(tenant_id, operation_id, actual_micro, receipt_ref, True, connection)

    def _settle(self, tenant, operation, actual, receipt, reconcile, connection) -> Operation:
        _amount(actual)
        _text(receipt, "receipt_ref")
        with self._tx(connection) as conn:
            row = self._get(conn, tenant, operation, lock=True)
            if row["status"] == Status.SETTLED:
                if row["actual_micro"] != actual or row["receipt_ref"] != receipt:
                    raise Conflict("Operation already settled with a different amount or receipt")
                return self._operation(row)
            expected = Status.UNKNOWN if reconcile else Status.DISPATCHED
            if row["status"] != expected:
                raise InvalidTransition(f"Expected {expected}; got {row['status']}")
            self._finish_balances(conn, row, actual)
            row = self._one(
                conn,
                """UPDATE {s}.operations SET status='settled', actual_micro=%s, receipt_ref=%s
                WHERE tenant_id=%s AND operation_id=%s RETURNING *""",
                (actual, receipt, tenant, operation),
            )
            self._event(
                conn,
                row,
                "reconciled" if reconcile else "settled",
                {
                    "actual_micro": actual,
                    "over_bound": actual > row["bound_micro"],
                    "receipt_ref": receipt,
                },
            )
            return self._operation(row)

    def cancel_before_dispatch(
        self,
        tenant_id: str,
        operation_id: UUID,
        *,
        connection: Connection | None = None,
    ) -> Operation:
        with self._tx(connection) as conn:
            row = self._get(conn, tenant_id, operation_id, lock=True)
            if row["status"] == Status.CANCELLED:
                return self._operation(row)
            if row["status"] != Status.RESERVED:
                raise InvalidTransition("Only an undispatched reservation can be cancelled")
            self._finish_balances(conn, row, 0)
            row = self._set_state(conn, row, Status.CANCELLED)
            self._event(conn, row, "cancelled", {})
            return self._operation(row)

    def mark_unknown(
        self,
        tenant_id: str,
        operation_id: UUID,
        *,
        reason: str,
        connection: Connection | None = None,
    ) -> Operation:
        _text(reason, "reason")
        with self._tx(connection) as conn:
            row = self._get(conn, tenant_id, operation_id, lock=True)
            if row["status"] == Status.UNKNOWN:
                return self._operation(row)
            if row["status"] != Status.DISPATCHED:
                raise InvalidTransition("Only a dispatched operation can become unknown")
            row = self._set_state(conn, row, Status.UNKNOWN, reason)
            self._event(conn, row, "unknown", {"reason": reason})
            return self._operation(row)

    def recover_expired(self, tenant_id: str, *, limit: int = 100) -> list[Operation]:
        """Run from an application scheduler. Each operation recovers in its own transaction."""
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise ValueError("limit must be an integer between 1 and 10000")
        with self._tx() as conn:
            candidates = self._all(
                conn,
                """SELECT operation_id FROM {s}.operations WHERE tenant_id=%s
                AND status IN ('reserved','dispatched') AND expires_at <= clock_timestamp()
                ORDER BY expires_at, operation_id LIMIT %s""",
                (tenant_id, limit),
            )
        recovered = []
        for candidate in candidates:
            with self._tx() as conn:
                row = self._get(conn, tenant_id, candidate["operation_id"], lock=True)
                if row["status"] not in (Status.RESERVED, Status.DISPATCHED):
                    continue
                if not self._one(
                    conn,
                    "SELECT clock_timestamp() >= %s AS expired",
                    (row["expires_at"],),
                )["expired"]:
                    continue
                if row["status"] == Status.RESERVED:
                    self._finish_balances(conn, row, 0)
                    row = self._set_state(conn, row, Status.CANCELLED)
                    self._event(conn, row, "expired_before_dispatch", {})
                else:
                    row = self._set_state(conn, row, Status.UNKNOWN, "lease_expired")
                    self._event(conn, row, "unknown", {"reason": "lease_expired"})
                recovered.append(self._operation(row))
        return recovered

    def events(self, tenant_id: str, operation_id: UUID) -> list[dict]:
        with self._tx() as conn:
            self._get(conn, tenant_id, operation_id)
            return self._all(
                conn,
                """SELECT event_id, kind, details, created_at FROM {s}.events
                WHERE tenant_id=%s AND operation_id=%s ORDER BY event_id""",
                (tenant_id, operation_id),
            )
