# Code review findings

## 1. Critical — account debits and credits are non-atomic, allowing overspend and lost refunds

**File:** `internal/service/account.go:79` (also `internal/service/account.go:118`, `internal/store/memory/memory.go:182`)

`Debit` reads an account, performs the balance/limit checks outside the store lock, and later overwrites the whole account with `PutAccount`. Two concurrent submissions for the same principal can therefore both observe a $25 balance and $20 remaining limit, both approve a $15 debit, and both write back $10/$5. Two escrows totaling $30 have then been created while the account records only one $15 debit. Concurrent credits/debits can similarly overwrite one another and lose a refund.

**Suggested fix:** Move the read/check/update into one atomic store operation (for example, `DebitAccount`/`CreditAccount` under the memory store mutex, and a transaction or conditional update in a database implementation). Do not expose financial read-modify-write as separate `GetAccount` and `PutAccount` calls.

## 2. Critical — cancellation races the worker and can overwrite terminal state or settle a canceled job

**File:** `internal/service/job.go:195` and `internal/service/job.go:271`

The background pipeline and `Cancel` independently read and overwrite the entire job without a compare-and-swap transition. For example, `Cancel` can release the escrow and write `canceled`; the worker then fails verification, calls `fail`, and overwrites it with `failed`. In the opposite ordering, settlement can finish between `Cancel`'s terminal-state check and its ignored `ReleaseEscrow` error; `Cancel` then writes `canceled` even though funds were charged. There is also no call to `provider.CancelJob`, so an execution already in progress is not actually asked to stop.

**Suggested fix:** Implement atomic state transitions with expected source states/versioning. Cancellation should atomically claim the job before releasing escrow, signal the provider, and prevent verify/settle from proceeding. Completion/failure must refuse to overwrite a terminal state, and release/settle errors must not be ignored.

## 3. High — idempotency reservations are permanently poisoned by failures and concurrent retries

**File:** `internal/service/job.go:83` and `internal/store/memory/memory.go:199`

`submit` reserves the key before validating the quote/capability, checking policy, debiting, or creating the job. Every error before `Bind` leaves a record whose `ResponseKey` is empty. A retry with the same request hash takes the replay path and calls `GetJob("")`, returning a store error instead of retrying or returning the original domain outcome. The same happens when an identical request arrives concurrently while the first request is still in progress.

**Suggested fix:** Give idempotency records explicit `in_progress`, `completed`, and `failed` states and persist the complete response/error. Identical concurrent callers should wait for or observe the first result. Alternatively, release a reservation on every pre-commit failure, but retain completed financial outcomes for the required retry window.

## 4. High — `input_required` binds idempotency to a job that was never stored, and there is no usable confirmation continuation

**File:** `internal/service/job.go:122`

The confirmation branch constructs a job and calls `Bind`, but never calls `PutJob`. Replaying the same request therefore resolves the idempotency record to a nonexistent job. Reissuing with the same key after approval cannot continue either: it takes that broken replay path, while using a new key simply returns `input_required` again because no confirmation/request state is accepted or validated. Thus every quote above the autonomous limit is permanently unexecutable through both REST and MCP.

**Suggested fix:** Persist the input-required job before binding it, return an opaque signed request state/confirmation request, and add a continuation path that validates approval and quote expiry then atomically advances that same job. Replays should return the stored input-required result.

## 5. High — refunds restore balance but never restore the daily allowance

**File:** `internal/service/account.go:118`

`Debit` reduces both balance and `RemainingToday`, but `Credit` only increases balance. A provider/verification failure or cancellation that refunds the escrow in full still consumes the user's daily limit. Repeated failed $1 jobs can reduce `RemainingToday` to zero despite no money being charged; partial settlement refunds likewise count the reserved maximum rather than actual spend.

**Suggested fix:** Distinguish reserved funds from settled spend. Track reservations separately, or have release/refund atomically restore both available balance and the corresponding daily allowance; on settlement, consume only the actual charged amount.

## 6. High — `Invoke` ignores `MaxWaitMS` and always blocks through the entire pipeline

**File:** `internal/service/job.go:177`

For the inline path, `MaxWaitMS` is never read. `runToCompletion` executes synchronously and the method cannot return `accepted` when the wait budget expires, contrary to its API contract. With a slow real adapter—or simply a provider that ignores context—an invocation can hold the HTTP/MCP request indefinitely. A zero `max_wait_ms` also runs synchronously even though the field's comment says zero means “don't wait.”

**Suggested fix:** Run the pipeline independently and wait on a completion channel/select bounded by `MaxWaitMS` and request cancellation. Return the latest persisted job as `accepted` when the deadline expires while allowing the claimed worker to continue exactly once.

## 7. Medium — quote version immutability is not enforced at execution time

**File:** `internal/service/job.go:113`

The quote records `CapabilityVersion`, but submission only checks the capability ID and then executes whatever capability record is currently stored. If a capability is replaced or later update support changes its version/provider/schema/pricing, an old quote can execute against different terms while settlement still charges the old quote. This defeats the spec guarantee that quote/job records retain the version they were created against.

**Suggested fix:** Require the current executable capability version (and immutable terms hash inputs) to match the quote, or store an immutable capability-version snapshot referenced by the quote and execute that snapshot.

## 8. Medium — quote constraint currency is silently discarded

**File:** `internal/httpapi/quotes.go:27` and `internal/mcp/tools.go:155`

Both transports extract only `constraints.max_total.amount`; the supplied currency is ignored. A client sending a EUR 8.00 ceiling for a USD-priced capability has it silently interpreted as USD 8.00. Missing or malformed currency is also accepted, so the server may issue a quote under a materially different constraint than the caller supplied.

**Suggested fix:** Carry `max_total` as a full money value into `CreateQuoteInput`, require amount and currency, validate the currency against the capability price, and reject malformed/mismatched constraints.

## 9. Medium — receipt verification is not bound to the job/provider being settled

**File:** `internal/adapters/toscore/mock/mock.go:130`

Verification checks only capability ID plus nonempty signature/output hash, then marks the escrow verified. It does not check `receipt.JobID`, `receipt.ProviderID`, or the input/output hashes against the escrow/job. `SettleJob` subsequently checks only that some receipt was recorded for the escrow and accepts an independently supplied `JobID`. A receipt from another job for the same capability can therefore authorize settlement and produce a receipt attributed to an unrelated job.

**Suggested fix:** Bind the verified record to all settlement-critical fields (escrow, job, provider, capability, input hash, output hash), validate them during verification, and require `SettleJob` to match that exact verified receipt. Clear/consume the verification record atomically on terminal transition.
