"""Domain failures must survive transport from worker processes."""

import multiprocessing
import pickle
from concurrent.futures import ProcessPoolExecutor

import pytest

from quotalatch import BudgetExceeded


def _reject_reservation():
    raise BudgetExceeded("customer", 30_000, 20_000)


def test_budget_exceeded_pickle_round_trip_preserves_details_and_notes():
    original = BudgetExceeded("customer", 30_000, -20_000)
    original.add_note("Rejected by the background worker")

    restored = pickle.loads(pickle.dumps(original))

    assert type(restored) is BudgetExceeded
    assert restored.budget_id == original.budget_id
    assert restored.requested_micro == original.requested_micro
    assert restored.available_micro == original.available_micro
    assert restored.args == original.args
    assert restored.__notes__ == original.__notes__
    assert str(restored) == str(original)


def test_process_pool_propagates_budget_exceeded_to_caller():
    with ProcessPoolExecutor(
        max_workers=1, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        future = pool.submit(_reject_reservation)
        with pytest.raises(BudgetExceeded) as caught:
            future.result(timeout=30)
        assert pool.submit(int, "42").result(timeout=30) == 42

    assert caught.value.budget_id == "customer"
    assert caught.value.requested_micro == 30_000
    assert caught.value.available_micro == 20_000
