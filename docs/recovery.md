# Recovering a QuotaLatch operation

Use this runbook after a worker crash, a lost database response, or an ambiguous
provider outcome. A dispatch claim records permission to start a call; it does
not record whether the provider received it or charged for it.

## Look up the original attempt

Use the tenant from your trusted application context and the original operation
key. Call `find_operation(tenant_id, operation_key)` against the authoritative
database. Use `events(tenant_id, operation_id)` to see the ledger's transitions.
The journal can explain why a hold remains, but it is not provider evidence.

If lookup returns `NotFound` after an interrupted reservation, retry
`reserve` with the **same key and identical inputs**, including budget scopes,
bound, fingerprint, and TTL. This also handles a reservation that commits
between the lookup and the retry. Never call the provider on the strength of a
reservation or lookup alone.

| Current status | What to do |
| --- | --- |
| `reserved` | If work should proceed, use the stored token with `mark_dispatched`. Only a successful claim whose commit was confirmed authorizes the provider call. If the reservation has expired, let recovery cancel it. |
| `dispatched` | Check whether the original worker is still running. For an ambiguous outcome, look up the original attempt using the provider's idempotency key or receipt reference. A second dispatch claim will be denied. |
| `unknown` | Keep the hold while you look for evidence. Use `reconcile` once you can verify the actual charge, including verified zero. |
| `settled` | The charge is recorded. If the settlement response was lost, replay the same amount and receipt reference; a conflicting result needs investigation. |
| `cancelled` | The reservation ended before dispatch and its hold was released. If the application still needs to start a new attempt, give that attempt a new key. |

A failed or lost dispatch response is not permission to call the provider.
Even if a lookup shows `reserved`, obtain a fresh successful claim before making
the call. A timeout, lease expiry, or absence of a receipt does not establish
that the original attempt was free.

## Record verified usage

Persist the verified amount in integer micro-USD and a stable reference to the
evidence. Use `settle` for a dispatched operation and `reconcile` for an unknown
operation. If your worker knows the provider outcome is ambiguous, it can call
`mark_unknown` while the operation is still dispatched. Use a short reason code
and keep prompts, credentials, and personal data out of the ledger.

Recovery can move an operation from dispatched to unknown just as the worker
receives the receipt. If settlement raises `InvalidTransition`, reread the
operation. If it is now unknown, reconcile with the same verified amount and
reference. If it is already settled, verify that the stored amount and reference
match. If they disagree, investigate the discrepancy before taking another action.
Without reliable evidence, leave the operation unknown.

## Run scheduled recovery

Call `recover_expired(tenant_id, limit=100)` periodically for each tenant your
application manages. It releases holds for expired, undispatched work and marks
expired, dispatched work unknown without releasing its hold. Track unresolved
unknown operations in your application so they can be investigated later.

Each operation recovers in its own transaction. If a batch fails midway, earlier
transitions may already be committed; a later run can continue safely. Concurrent
sweeps may return fewer results than the requested limit because another worker
already changed a candidate. Keep the sweep on its normal schedule.

To see these cases with a simulated provider, run the [crash-and-retry
demo](../README.md#run-locally).
