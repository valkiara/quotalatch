from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class Status(StrEnum):
    RESERVED = "reserved"
    DISPATCHED = "dispatched"
    UNKNOWN = "unknown"
    SETTLED = "settled"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class Budget:
    tenant_id: str
    budget_id: str
    limit_micro: int
    spent_micro: int
    held_micro: int

    @property
    def available_micro(self) -> int:
        return self.limit_micro - self.spent_micro - self.held_micro

    @property
    def over_limit(self) -> bool:
        return self.available_micro < 0


@dataclass(frozen=True)
class Operation:
    tenant_id: str
    operation_id: UUID
    operation_key: str
    request_fingerprint: str
    status: Status
    bound_micro: int
    actual_micro: int | None
    dispatch_token: UUID
    expires_at: datetime
    receipt_ref: str | None
    unknown_reason: str | None
