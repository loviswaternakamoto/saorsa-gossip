# ADR-015: Step-by-Step JOIN API with a Caller-Held Retry Ledger

- **Status:** Proposed
- **Date:** 2026-10-12
- **Decision owners:** David Irvine
- **Reviewers:** <pending>
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [x0x #1321](https://github.com/saorsa-labs/x0x/issues/1321); x0x rulings D248 (the JOIN driver stays in saorsa-gossip) and D252 (exponential loss backoff, owned by saorsa-gossip); [ADR-001](ADR-001-protocol-layering.md)

## Context

`Membership::join(seeds)` dials each seed with `dial_bootstrap` and sends the HyParView JOIN frame to the peer that answered. It gives each seed-list entry ten attempts. Only a failed dial grows the backoff (100 ms, doubled, at most 30 s, plus jitter).

x0x must not send the JOIN frame to a peer that its gossip-plane gate has not admitted (x0x #206, #292). The plane verdict arrives after the dial. So x0x must decide between the dial and the frame. `join` offers no point for this.

x0x also changes candidates during one join. A seed that loses its connection can come back later, or another seed can become admitted. Each seed address must keep one budget and one backoff for the whole join. `join` keeps its state inside one call, one seed at a time.

A copy of the JOIN loop in x0x was rejected (D248): the driver, the frame and the side effects stay here.

## Decision Drivers

- The JOIN frame, its side effects and the retry policy stay in one place: saorsa-gossip.
- A caller can check admission after the dial and before the frame.
- One budget per seed address for a whole join, across candidate changes.
- Cancellation must not leave the local views half updated.
- `Membership::join` keeps its signature and behaviour.

## Considered Options

1. A step API: `join_dial` and `join_send`, with a caller-held `JoinLedger`.
2. `join` with an async gate callback.
3. A retry-count argument on `join`.

## Decision

We will add option 1 to `saorsa-gossip-membership`, as additive public API:

- `JoinLedger` holds, per seed address, the attempts used, the dial backoff, the loss cooldown and the readiness time.
- `HyParViewMembership::join_dial(&mut JoinLedger, seed) -> JoinDial` makes one `dial_bootstrap`. It returns `Answered(peer)`, `Failed { retry_after, exhausted }`, `NotReady { ready_at }` or `Exhausted`.
- `HyParViewMembership::join_send(seed, peer) -> Result<JoinSend>` stores the seed address, sends the JOIN frame and adds the peer to the active view. It returns `Joined` or `SendFailed`. An active-view error is returned.
- `JoinLedger::record_loss(seed)` starts the loss cooldown (D252).

### Caller obligations and guarantees

1. **Ledger lifetime.** Keep one ledger for one whole join. A new ledger is a new budget.
2. **Exhaustion.** A seed address has `JOIN_MAX_ATTEMPTS` (10) attempts per ledger. After that, `join_dial` returns `Exhausted` and makes no dial. A ledger never renews a budget.
3. **Repeated seed entries.** In the step API, a repeated address shares one budget. `Membership::join` keeps one fresh state per seed-list entry, as before.
4. **Waiting.** After `Failed`, the caller waits `retry_after`, or `JoinLedger::ready_at(seed)`. `join_dial` enforces this: before `ready_at` it returns `NotReady` and uses no attempt. The caller may dial another seed meanwhile.
5. **Loss cooldown (D252).** If the answered peer loses its connection before the caller sends the frame, the caller calls `record_loss(seed)`. The cooldown is 100 ms, doubled after each loss, at most 30 s, plus the same jitter. Answered dials do not reset it. Readiness is the later of the dial backoff and the cooldown; the delays are not added. A failed send (`SendFailed`) still allows the next attempt at once. Only a failed dial grows the dial backoff.
6. **Cancellation.** Both futures can be dropped; a runtime shutdown drops them in the same way. A dropped future gives no result.
   - A dropped dial counts as an attempt and does not change the backoff.
   - Until the transport's send returns `Ok`, a dropped `join_send` leaves only the stored address. The transport can have sent the frame before that, so the seed can have the JOIN.
   - Then the local commit (the active-view insert, a demotion to the passive view when the view is full, and the SWIM alive mark) takes every lock that it needs before its first change: the active view, the passive view when a demotion is due, then the SWIM state. After that it does not await. A drop while it waits for a lock changes nothing: the seed can have the JOIN while this node does not list the seed (as a drop at the same point of the 0.5.87 `join`). No executor can stop the commit between its first and its last change. If a lock never becomes free, the future waits.
   - `Membership::add_active` uses the same commit.
7. **Runtime.** `join_dial` and `join_send` spawn no task and start no timer, so they run on any executor; the transport can have its own needs. As in 0.5.87, `HyParViewMembership::new` needs a Tokio runtime (it starts background tasks), and `Membership::join` sleeps for its backoff with a Tokio timer. Its success path needs no entered runtime.
8. **Legacy `join`.** `Membership::join` runs on the same steps, with a fresh ledger per entry. It never reports a loss. Its dials, frames, stored addresses, views, SWIM states, results and backoff are as in 0.5.87 (the differential test). Concurrent scheduling can differ, because the commit holds the active-view lock until it has the SWIM lock.

## Consequences

### Positive

- One implementation of the JOIN frame and its side effects.
- x0x applies its admission gate without a protocol copy.
- Budgets and backoffs survive candidate changes.

### Negative / Trade-offs

- A larger public API that callers must use correctly.
- The commit holds the active-view lock while it waits for the passive-view and SWIM locks. `active_view()` (a `try_read`) returns an empty list during that time, as it already did while the 0.5.87 `add_active` waited for the passive-view lock. No code path takes these locks in the reverse order, so there is no new lock-order cycle.

### Neutral / Operational

- Additive: 0.5.88 (workspace lockstep).

## Validation

- `join_budget_tests.rs`: a differential test against a verbatim copy of the 0.5.87 loop and `add_active` (dials, frames, stored addresses, views, SWIM state, results and backoff gaps); the ledger across candidates; backoff only on a failed dial; a gate refusal before the send; the loss cooldown; the readiness rule; early-dial refusal; safe drops; two lock-contention drop tests; a runtime shutdown while the commit waits for a lock; the legacy `join` polled outside a runtime; a full-view demotion against the 0.5.87 `add_active`.
- Review trigger: any change to `join`, `add_active` or the JOIN frame.

## Notes for AI-assisted work

AI tools may help draft this ADR, but **must not mark it Accepted without human review**. Accepted ADRs are immutable: create a new superseding ADR rather than editing an Accepted ADR.
