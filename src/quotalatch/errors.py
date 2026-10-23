"""Domain failures; database and connection failures propagate unchanged."""


class LedgerError(Exception):
    """Base class for ledger domain failures."""


class NotFound(LedgerError):
    """The resource does not exist in the supplied tenant."""


class Conflict(LedgerError):
    """An idempotency key or terminal result was reused with different data."""


class InvalidTransition(LedgerError):
    """The operation cannot perform the requested state transition."""


class DispatchDenied(InvalidTransition):
    """The operation is expired, already claimed, or has a stale dispatch token."""


class BudgetExceeded(LedgerError):
    def __init__(self, budget_id: str, requested_micro: int, available_micro: int):
        self.budget_id = budget_id
        self.requested_micro = requested_micro
        self.available_micro = available_micro
        super().__init__(
            f"Budget {budget_id!r}: requested {requested_micro} micro-USD, "
            f"available {available_micro}"
        )
