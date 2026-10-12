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
   - Then the local commit (the active-view insert, a demotion to the passive view when the view is full, and the SWIM alive mark) takes every lock that it needs before its first change: the SWIM state, the active view, and the passive view when a demotion is due. After that it does not await. A drop while it waits for a lock changes nothing: the seed can have the JOIN while this node does not list the seed. No executor can stop the commit between its first and its last change. If a lock never becomes free, the future waits.
   - This is deliberately different from 0.5.87. There, a drop while `add_active` waited for the passive-view lock left the demoted peer in neither view, and a drop while it waited for the SWIM lock left the new peer active but not alive.
   - Before the fallback, the commit does not hold one of these locks while it waits for another. Each round takes them with `try_write`; when one is busy, the round releases the others and waits for that one alone, then starts the next round holding it. A private `commit_gate` serializes commits. After eight busy rounds the commit waits in the fixed order (SWIM state, active view, passive view), holding the earlier ones; this bounds the retries.
   - `Membership::add_active` uses the same commit.
7. **Runtime.** `join_dial` and `join_send` spawn no task and start no timer, so they run on any executor; the transport can have its own needs. As in 0.5.87, `HyParViewMembership::new` needs a Tokio runtime (it starts background tasks), and `Membership::join` sleeps for its backoff with a Tokio timer. Its success path needs no entered runtime.
8. **Legacy `join`.** `Membership::join` runs on the same steps, with a fresh ledger per entry. It never reports a loss. For a run that completes, its dials, frames, stored addresses, views, SWIM states, results and backoff are as in 0.5.87 (the differential test). This is not a claim about cancellation (see 6) or about concurrent observations: the commit takes its locks differently and holds them together.

## Consequences

### Positive

- One implementation of the JOIN frame and its side effects.
- x0x applies its admission gate without a protocol copy.
- Budgets and backoffs survive candidate changes.

### Negative / Trade-offs

- A larger public API that callers must use correctly.
- `active_view()` uses `try_read` and returns an empty list while the active-view lock is held, and also while a writer waits for it: Tokio's lock is fair and reserves the read permits for the waiting writer, both while other tasks hold the lock and after they release it, until that writer runs again. A reader then sees no peers for that call: the coordinator advert sends nothing, an uncached FOAF lookup returns an empty result, and x0x stale-target maintenance skips pruning for that pass and can re-add a peer that is already active. Before the fallback, the commit hides the view only during its synchronous changes and while it waits for the active-view lock, which every writer of the active view in 0.5.87 also does; a wait for the SWIM or the passive-view lock does not hide it (0.5.87 hid it while a demoting `add_active` waited for the passive lock). In the fallback, a demoting commit holds the active-view lock while it waits for the passive-view lock, so `active_view()` returns an empty list for that whole wait. A snapshot behind `active_view()` would remove these windows; it is out of scope.
- Latency. The commit holds no lock while it waits, so a SWIM ACK, a probe-timeout write or a view reader does not wait for the commit's own waits. Each busy round costs one extra wait in the queue of the busy lock, behind work that another task is doing. Only after eight busy rounds does the commit wait in the fixed order, holding the earlier locks; then SWIM state readers can wait behind that commit's view-lock wait. The probe-timeout re-check keeps that case correct for an ACK that cleared its probe before the re-check: an ACK clears its probe before it marks the peer alive, so the re-check sees the cleared probe. An ACK that arrives after the timeout has taken the probe table waits for it; the timeout marks the peer suspect and asks for indirect probes, and the ACK then marks the peer alive.
- The probe-timeout re-check takes the probe table (read) and then the state lock, both in Tokio's fair queues. Busy writers can delay it but cannot make it skip a probe: the first scan that finds an overdue probe decides it. While it waits for the state lock, it holds the probe-table read, so probe recording and ACK clearing wait for it.
- Lock order for the waits that hold a lock: commit gate, SWIM state, active view, passive view (the commit fallback); active view, passive view (degree maintenance); probe table, SWIM state (the probe-timeout re-check). No code path holds a view lock while it waits for the SWIM lock, and none holds the SWIM state while it waits for the probe table. So there is no lock-order cycle.

### Neutral / Operational

- Additive: 0.5.88 (workspace lockstep).

## Validation

- `join_budget_tests.rs`: a differential test against a verbatim copy of the 0.5.87 loop and `add_active` (dials, frames, stored addresses, views, SWIM state, results and backoff gaps); the ledger across candidates; backoff only on a failed dial; a gate refusal before the send; the loss cooldown; the readiness rule; early-dial refusal; safe drops; two lock-contention drop tests; a runtime shutdown while the commit waits for a lock; the legacy `join` polled outside a runtime; a full-view demotion against the 0.5.87 `add_active`; `active_view()` while a commit waits for the SWIM lock and while a demoting commit waits for the passive lock; an ACK before a probe-timeout scan while a commit waits for a view lock; an ACK between the timeout scan and its re-check; repeated probe-table contention at four scans (the first scan suspects); the retry bound after eight busy rounds. The SWIM tests use a paused Tokio clock, lock guards and a probe backdated on the std clock, with no wall-clock margins.
- Review trigger: any change to `join`, `add_active` or the JOIN frame.

## Notes for AI-assisted work

AI tools may help draft this ADR, but **must not mark it Accepted without human review**. Accepted ADRs are immutable: create a new superseding ADR rather than editing an Accepted ADR.
