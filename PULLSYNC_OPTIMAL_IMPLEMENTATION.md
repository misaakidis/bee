# Implementation brief — pull-sync optimal design

> **Audience:** the engineer/agent implementing the PR. This file is scaffolding for the
> work, not part of the shipped product — **delete it before the PR is opened for review.**
>
> **Source of truth for the design:** `pullsync-optimal-design.md` (the SWIP doc). Section
> references below (§5.1 etc.) point into it. When this brief and the doc disagree, the doc
> wins — flag the discrepancy rather than guessing.

---

## 0. What this PR does, in one paragraph

Today every node runs an independent pull-sync session against *each* of its `k`
neighbourhood peers, with no coordination. When several peers hold the same missing chunk
(the common case under θ-replication) all sessions independently `Want` it, so one missing
chunk is delivered up to `k` times. This PR replaces the independent per-peer sessions with a
single **per-chunk scheduler**: each missing chunk is fetched **exactly once**, from one
holder chosen among the ≥2 that offer it, with explicit failover to another holder if the
chosen one stalls. The wire protocol (`pkg/pullsync/pb`) does **not** change; the puller and
the `pullsync` Go API are reworked. This is the bandwidth saving SWIP-25 targets — up to `k×`
fewer chunk deliveries — landing over the existing Offer/Want transport.

This implements **Part I** of `pullsync-optimal-design.md` (§3–§10). Part II (§11–§12) is
analysis, not code. The advertisement-bandwidth upgrade (§7, set reconciliation) is explicitly
**out of scope and deferred** — we keep Offer/Want.

**The scheduling logic is built as a state machine that is a 1:1 hand-translation of the verified
TLA+ model `optimal-testbed/PullSyncerE.tla`** (§4) — same variables, actions, guards, and knobs —
so the running protocol does what the model checker proved. The same knobs let the Go tests run the
same ablation matrix as `optimal-testbed/run.sh` (§8.1).

---

## 1. Scope decisions (already made — do not relitigate)

| Decision | Choice | Consequence |
|---|---|---|
| Fidelity | **Full per-chunk** design (§5.1–§5.6) | The scheduler operates on individual chunk addresses, not whole bins. |
| Branch base | **`master`** (this branch is `pullsync-optimal-design`, freshly cut from `origin/master`) | Do **not** port from the `swip-25d-pullsyncer` branch — see §9. |
| `pkg/pullsync` blast radius | **Go API may change; the wire `.proto` may not** | `Sync` is decomposed into `Offer`+`Fetch` Go methods; `pb/pullsync.proto` and `protocolVersion` stay byte-identical. |
| Deliverable shape | brief + **failing test scaffold** | The scaffold (§8) encodes acceptance criteria and fails until you implement. |

**Out of scope:** set reconciliation / rateless advertisement (§7); the stamp-only fetch and
cross-batch byte-dedup (§11); any postage/reserve/storer change; any `pb` change; any
redistribution-game work (§12).

---

## 2. The design, mapped to bee (§5 of the doc → code)

Five mechanisms. Three are **gate-critical** (omitting one is a correctness bug, machine-checked
in the doc's §9 ablation table); two are **floor-achieving** (omitting one keeps correctness but
loses an optimality bound). Plus the LIVE obligation (§5.6).

| # | Doc | Mechanism | Where it lives in this PR | Tier |
|---|---|---|---|---|
| 1 | §5.1 | **Multi-source ≥2**: each missing chunk has ≥2 candidate holders; one fetch at a time, the rest are fallbacks | scheduler: `holders[chunk] = set of peers that offered it` | gate-critical (O1, O6) |
| 2 | §5.2 | **Chunk-level dedup**: one shared, **chunk-address-keyed** in-flight set; check-and-mark is **one indivisible step** | scheduler: `inFlight map[address]peer`, consulted+set atomically before issuing a `Want` | gate-critical (O3) |
| 3 | §5.4 | **Failover-with-exclude**: on stall, try the next holder and **permanently exclude** the staller *for that chunk* | scheduler: `excluded[chunk] = set of peers barred for it` | gate-critical (O1, O6) |
| 4 | §5.3 | **Load-aware routing**: among a chunk's holders, fetch from the **least-loaded**; ties broken by proximity (latency) | scheduler: per-peer outstanding-fetch counter; pick `argmin load` over `holders[c] \ excluded[c]` | floor-achieving (O5) |
| 5 | §5.5 | **Deepest-first**: fetch the deepest (highest-PO, nearest) bins first | scheduler: iterate bins high→low | floor-achieving (O2) |
| 6 | §5.6 | **LIVE**: every chunk arriving after the cursor is also pulled | per-bin live subscription continues past the cursor | regime obligation |

The check-and-mark **atomicity** (mechanism 2) is not optional: the doc's §9 `MC_nonatomic`
ablation shows that splitting it into *test* then *mark* lets two peers both pass the test and
double-deliver. In Go this means the in-flight set must be guarded so that *"is `c` claimed? if
not, claim it for peer `p`"* is one critical section. Since the scheduler is driven by a single
control goroutine (see §4), this falls out for free — **keep it that way**; do not add a second
mutator of the in-flight set. The test `TestOptimal_ConcurrentWantsDedupToOneFetch` (§8) pins
this: two holders offering the same chunk must produce **one** want in flight, hence one
delivery; splitting check-and-mark is the doc's `MC_nonatomic` double-fetch.

### The `(address, batchID, stampHash)` triple vs. the address (read §5.2 and §11 of the doc)

- **Dedup / move** keys on the **chunk address** (content self-verifies; identical bytes move
  once). This is what mechanism 2 dedups on.
- **Completeness / "do I have it"** keys on the full **triple** — `storer.ReserveHas(addr,
  batchID, stampHash)`. The reserve is postage-accounted per triple.

So: the *local-have* check and the *want* decision use the triple (as today), but the
*cross-peer in-flight* dedup keys on the address. In the full-replication common case the triple
and the address coincide; the divergence (re-stamped content) is the §11 deferred item — **do not
implement the stamp-only fetch here**, just don't let the address-keyed in-flight set incorrectly
suppress a genuinely-needed different-triple entry. Practical rule: in-flight set keyed on
address is a *delivery* dedup (don't fetch the same bytes twice concurrently); the per-triple
`ReserveHas` is the *completeness* check. A chunk is "missing" iff `ReserveHas(triple)==false`;
it is "fetchable now" iff missing **and** its address is not already in-flight.

---

## 3. Protocol decomposition (`pkg/pullsync`) — Go API only, wire unchanged

### 3.1 Why decompose

The current `Sync(ctx, peer, bin, start)` does the whole `Get→Offer→Want→Delivery` cycle and
decides what to `Want` purely from a **local-store** check (`pullsync.go`, the `ReserveHas`
loop). It has no visibility of what *other* peers are offering, so it cannot route per chunk.
The per-chunk scheduler needs to (a) learn each peer's offer for a bin, (b) decide per chunk
which peer fetches it, (c) fetch only the assigned subset from each peer. That requires
splitting discovery from fetching.

### 3.2 The wire constraint (read `pullsync.go` `handler` + `Sync` first)

On the wire, one stream is `Get → Offer → Want(bitvector) → Delivery*`. The `Want` bitvector
indexes positionally into the `Offer` **on the same stream** — the server holds the offer in
memory keyed to that stream. So you cannot offer on stream A and want on stream B.

**Recommended approach (clean, mockable): re-offer on fetch.**

- `Offer(ctx, peer, bin, start)` opens a stream, sends `Get`, reads the `Offer`, then closes the
  stream (send an empty `Want` so the server returns cleanly, or `Reset` — match whatever the
  handler tolerates without logging errors; verify against `handler`). Returns the offered chunk
  descriptors + `Topmost`.
- `Fetch(ctx, peer, bin, start, want []ChunkRef)` opens a **fresh** stream, runs the full
  `Get→Offer→Want→Delivery` cycle but sets the `Want` bits only for the `want` subset (matched
  against the fresh offer by the `(address, batchID, stampHash)` triple), verifies + stores
  deliveries exactly as `Sync` does today, and returns the count delivered + error.

This costs one extra `Offer` round-trip per fetch, which is cheap: an offer descriptor is ~32–80
bytes vs. a ~4 kB chunk, and §7 of the doc establishes advertisement is ~128× cheaper than
delivery. It keeps the `pullsync.Interface` free of open-stream session objects, so the mock
stays a plain in-memory stub.

> **Optional optimization (only if profiling demands it):** an `OfferSession` that holds the
> stream open between `Offer` and `Fetch`, avoiding the second offer. This leaks a stream handle
> across the `Interface` and complicates the mock — **do not do it in the first PR.** Note it in
> the PR description as a follow-up.

### 3.3 New `pullsync.Interface`

```go
// ChunkRef identifies one offered reserve entry. It is the offer-side projection of
// storer.BinC (the wire Chunk message), carried back to the puller so it can decide
// per-chunk routing before fetching.
type ChunkRef struct {
    Address   swarm.Address
    BatchID   []byte
    StampHash []byte
    BinID     uint64 // the offering peer's per-bin sequence number (peer-local; see §5)
}

type Interface interface {
    // Offer returns the chunks peer holds in bin at/after start (one Get→Offer cycle),
    // and the offer's topmost BinID. No chunks are fetched.
    Offer(ctx context.Context, peer swarm.Address, bin uint8, start uint64) (chunks []ChunkRef, topmost uint64, err error)

    // Fetch downloads exactly the want subset from peer (a fresh Get→Offer→Want→Delivery
    // cycle wanting only want), verifies and stores them, and returns the number stored.
    Fetch(ctx context.Context, peer swarm.Address, bin uint8, start uint64, want []ChunkRef) (count int, err error)

    // GetCursors is unchanged.
    GetCursors(ctx context.Context, peer swarm.Address) ([]uint64, uint64, error)
}
```

Implementation notes:
- Factor the shared body of today's `Sync` (offer parsing, bitvector, delivery verify via
  `validStamp` + `cac.Valid`/`soc.FromChunk`, `ReservePutter().Put`, the `ErrUnsolicitedChunk`
  and `ErrOverwriteNewerChunk` handling) into helpers reused by `Offer`/`Fetch` — don't
  duplicate the verification logic.
- Keep the per-peer server-side rate limit (`handleRequestsLimitRate`) and `pageTimeout`
  behaviour exactly as is.
- You may keep a thin `Sync` if anything else depends on it — **grep for callers** (`pkg/node`,
  tests, `pkg/pullsync/mock`). If nothing outside the puller uses it, remove it.
- Update `pkg/pullsync/mock/pullsync.go` to implement `Offer`/`Fetch`. Keep the existing
  `Option`/`optionFunc` pattern and the `var _ pullsync.Interface = (*PullSyncMock)(nil)` check.

---

## 4. The per-chunk state machine — a 1:1 translation of `PullSyncerE.tla`

**This is the core ask of the PR: the scheduling logic is structured as a state machine that is a
direct, line-traceable hand-translation of the verified TLA+ model, so the running protocol
provably does what the model checker proved correct.** Same variables, same actions, same guards,
same knobs. The model is the spec; the Go is the refinement.

The TLA source lives in the SWIP repo at `optimal-testbed/PullSyncerE.tla` (per-chunk model) and
`optimal-testbed/PullSyncerNA.tla` (the atomicity companion); `optimal-testbed/run.sh` is the
config matrix (six positives, seven ablations). **Read those two `.tla` files before writing code
— they are short (≈150 and ≈70 lines) and they ARE the design.** Mirror the swip-25d branch's
craftsmanship here (a pure scheduler type with a TLA→Go mapping table in its doc comment) but
translate `PullSyncerE`, not the bin-level `PullSyncer`.

### 4.1 The refinement principle (why this is sound)

The model's `Next` is a non-deterministic choice among all enabled actions; TLC explores every
choice and proves `ConflictFree` (safety) on all of them and `Completeness`/`Freshness` (liveness)
under weak fairness. The implementation **resolves that non-determinism with a deterministic
policy** — deepest-first ordering and least-loaded routing (§4.4) — but **every step the
implementation takes is an enabled model action.** Because the implementation's behaviours are a
*subset* of the model's, every safety property proven for the model holds for the implementation
unchanged. The only thing a policy can break is *liveness* (by starving an enabled action forever),
so the one obligation the refinement adds is **fairness**: every chunk that is `Claimable` from
some honest holder must *eventually* get a `Want`. Keep the policy fair (finite bins, every missing
chunk eventually routed) and `Completeness`/`Freshness` carry over too.

> Practical consequence, and the headline test strategy (§8): because the Go state machine has the
> **same knobs** as the model, the Go unit tests run the **same matrix as `run.sh`** — flip a knob
> off and assert the same property breaks. "Runs as in the TLA models" stops being a slogan and
> becomes a green test.

### 4.2 The pure state machine (`pkg/puller/internal/pullstate`)

A pure type — no I/O, no goroutines, no locks, **not safe for concurrent use** (one goroutine
drives it, exactly like the swip-25d scheduler). Variables map 1:1 to `PullSyncerE`:

| `PullSyncerE.tla` | `pullstate` (Go) | meaning |
|---|---|---|
| `got ⊆ Chunks` | `got map[addr]struct{}` (mirrors `ReserveHas`) | fetched + stored (terminal) |
| `want ∈ [Chunks→SUBSET Peers]` | `want map[addr]peerKey` (Dedup ⇒ ≤1) | the in-flight **claim set** (§5.2) |
| `failed ∈ [Chunks→SUBSET Peers]` | `excluded map[addr]set[peerKey]` | per-chunk failover log (§5.4) |
| `arrived ⊆ Chunks` | `arrived map[addr]…` | available to fetch (HIST at init; LIVE via `NewChunk`) |
| `conflict ∈ BOOLEAN` | `conflict bool` | **must stay false**; latched on any double-deliver — a bug detector |
| `Holds[p]` (fixed) | `holders map[addr]set[peerKey]` (built from Offers) | who holds/offered each chunk |
| `Prio[c]` | bin number (deeper = higher) | priority key for `PrioOK` |

Plus the per-peer routing state the model abstracts away: `load map[peerKey]int` (outstanding
assigned fetches, for §4.4 least-loaded). And, as today, the persisted per-`(peer,bin)`
`intervalstore.Intervals` high-water (HIST resume; keep `IntervalPrefix="sync_interval"`, no
migration).

The knobs are a `Config` mirroring the `CONSTANT`s — **production defaults in bold**, the others
exist so the tests can ablate:

```go
type Config struct {
    Dedup        bool // §5.2  — true
    Failover     bool // §5.4  — true
    Exclude      bool // §5.4  — true
    SingleSource bool // §5.1  — false (true is a *disqualified* ablation; never ship true)
    Priority     bool // §5.5  — true (deepest-first; correctness-neutral, so optional but on)
    EnableLive   bool // §5.6  — true
}
```

### 4.3 The actions (translate verbatim — same guards)

Each TLA action becomes one method; each conjunct becomes one guard. Do not paraphrase the guards —
copy them.

| `PullSyncerE` action | `pullstate` method | the guard (`Claimable` etc.) |
|---|---|---|
| `Want(c,p)` | `Want(addr, peer) (ok bool)` | `Claimable`: `c∈arrived ∧ c∈Holds[p] ∧ c∉got ∧ p∉want[c] ∧ p∉failed[c] ∧ (Dedup⇒want[c]={}) ∧ (SingleSource⇒p=Assign[c]) ∧ (c∈Live⇒EnableLive) ∧ PrioOK(c)` |
| `Deliver(c,p)` | `Deliver(addr, peer)` | `p∈Honest ∧ p∈want[c]`; sets `got∪={c}`, **latches `conflict` if `c∈got` already**, clears `want[c]` |
| `ByzStall(c,p)` | `Stall(addr, peer)` | `p∈want[c] ∧ Failover`; clears `want[c]`, and **if `Exclude`** adds `p` to `failed[c]` |
| `NewChunk(c)` | `NewChunk(addr)` | `c∈LiveChunks ∧ c∉arrived`; `arrived∪={c}` |
| `PrioOK(c)` | `prioOK(addr) bool` | `¬Priority ∨ ∀d∈arrived: Prio[d]>Prio[c] ⇒ Addressed(d)` (deepest-first) |

Translation notes that matter:
- **`Deliver`'s `conflict` latch is your runtime double-fetch detector.** In the model it's how the
  ablation fails; in production it must never fire — wire it to a metric/`logger.Error` and treat a
  trip as a bug. This is the `ConflictFree` invariant made observable.
- **"Byzantine" / "Honest" are not knowable at runtime.** The model labels a non-delivering peer
  Byzantine; the implementation just observes *a `Fetch` that didn't deliver `c`* and calls
  `Stall`. Any non-delivery (omission, timeout, error, peer gone) maps to `ByzStall`. An honest
  delivery maps to `Deliver`. You never classify peers — you react to outcomes.
- **`SingleSource` stays false in production.** It is the disqualified family (`MC_single_*`). Keep
  it only as a test knob to reproduce the disqualification.
- The state machine exposes a **`Step()`** (or `Claimable`-enumerator) that returns the set of
  enabled `Want`s as `Op`s for the I/O shell — mirror the swip-25d `Op`/`activateNext` shape
  (`OpStart`/`OpStop`/`OpLive` → here `OpFetch`/`OpStop`/`OpLive`). The shell never reaches into
  the state directly.

### 4.4 Resolving the model's non-determinism: routing & order as policy

`Step()` may return several enabled `Want(c,p)` (chunk `c` offered by several holders; several
chunks claimable). The model picks arbitrarily; the implementation picks deterministically:
- **Which chunk first:** deepest bin first — already enforced as the `PrioOK` *guard* (so it is part
  of the verified model: `MC_vicinity` shows it correctness-neutral). Within a bin, any order.
- **Which holder:** `p* = argmin load[p]` over `holders[c] \ excluded[c]` (§5.3), ties broken by
  proximity via `swarm.Proximity` / `swarm.DistanceCmp` (latency + network-wide balance). This is
  *not* in the model — it only chooses *among already-enabled* `Want`s, so it cannot violate safety;
  keep it fair so it cannot starve liveness.

When `Want(c,p*)` is accepted, the in-flight mark is set **inside the same method call** (the
`Dedup⇒want[c]={}` check and the `want[c]∪={p}` update are one step — §4.7). `load[p*]++`. The shell
then issues `OpFetch`.

### 4.5 The I/O shell drives the state machine (`pkg/puller/puller.go`)

Keep master's structural lesson: a **single control goroutine** owns the `pullstate` machine and the
worker set, and **never blocks on a network call**. (This is also why the legacy puller froze on a
radius decrease — a lock held across blocking I/O.) The shell is the bridge between the model's
abstract actions and the wire:

| shell event | becomes | state-machine call |
|---|---|---|
| peer connects / `Offer` returns | learn holdings; HIST chunks available | populate `holders`/`arrived`; `Step()` |
| `Offer` returns a post-cursor chunk | LIVE arrival | `NewChunk(addr)` then `Step()` |
| `OpFetch` → `pullsync.Fetch` succeeds for `c` | honest delivery | `Deliver(addr, peer)` |
| `Fetch` fails / times out / peer gone for `c` | non-delivery | `Stall(addr, peer)` |
| topology / radius change | re-tile | update radius; re-`Step()` (§4.7) |

`pullsync.Fetch` returns an aggregate count, not per-chunk outcomes. The clean mapping: **fetch one
peer's assigned subset; a `Fetch` error is a `Stall` for every `c` in that subset** (each then
re-routes to another holder next `Step()`), and the chunks it did deliver are `Deliver`ed (match by
address — deliveries carry their address). If you want crisper feedback, have `Fetch` return the
addresses it delivered (a Go-API addition, no wire change). **Pick one, document it, test it.**

Discovery detail: for each eligible peer `p` (proximity ≥ bin, bin ≥ radius) a worker calls
`Offer(p, bin, start_p)` with `p`'s persisted high-water `start_p`. `start_p` is **per-peer**
because BinIDs are assigned independently by each peer (a chunk is BinID 5 at one peer, 9 at
another). Dedup keys on **address** (global, correct); only the resume bookkeeping is per-peer —
see the interval invariant in §4.6.

### 4.6 The interval-advance invariant (correctness-critical — get this right)

A `(peer, bin)` interval records "I have synced everything peer `p` offered in this bin up to BinID
`X`." With cross-peer dedup, a chunk peer `A` offered may have been fetched from peer `B`. That is
fine: **advance `A`'s interval to its offer `Topmost` only once every chunk `A` offered in
`[start_A, Topmost]` is locally held** (fetched from anyone, or already present). If some chunk `A`
offered is still missing at round end (no holder, or all excluded), advance `A`'s interval only to
the **contiguous prefix of held BinIDs**, so the gap is retried next round. Never advance past a
still-missing chunk — that would silently drop it and break O1 (Completeness).

> This is the one place the dedup design interacts non-trivially with the legacy interval
> bookkeeping. The master puller sidesteps it (each peer fetches everything it offers). Write a
> dedicated test (§8, `TestIntervalAdvanceAfterCrossPeerFetch`).

### 4.7 Radius changes (keep master's behaviour)

- **Decrease** (bins re-enter the reserve): reset the affected bins' intervals so evicted/ignored
  chunks resync (master's `resetIntervals`). Re-tile per §4 of the doc.
- **Increase**: stop syncing bins that left the reserve.
- Reuse `storer.RadiusChecker.StorageRadius()`; react on topology change and a poll tick, as today.

### 4.8 The atomicity obligation (`PullSyncerNA` — the one thing the model demands of the code)

`PullSyncerNA.tla` isolates the single refinement obligation the design places on the
implementation: the in-flight **check-and-mark must be one critical section**. The model's `Want`
fuses the dedup check (`want[c]={}`) and the mark (`want[c]∪={p}`) into one indivisible step;
`PullSyncerNA` splits them into `Decide` (check) and `Commit` (mark) behind an `Atomic` knob and
shows `ConflictFree` breaks when `Atomic=FALSE` (two peers both pass the check on `want[c]={}`,
both mark, the chunk is delivered twice — TOCTOU).

In this design the obligation is discharged **structurally**: the `pullstate` machine is mutated by
exactly one goroutine (§4.5), so `Want`'s check-and-mark is atomic by construction. **Do not add a
second mutator of the claim set, and do not split the check from the mark across an `await`/channel
hop.** The `-race` test `TestOptimal_ConcurrentWantsDedupToOneFetch` (§8) is the guard. If you ever
must let multiple goroutines claim, the claim set needs a mutex held across check-and-mark — but the
single-goroutine design is simpler and is what the model assumes.

### 4.9 Misbehaviour / blocklist

Keep it simple and faithful to the doc. The gate-critical mechanism is **exclude-per-chunk**
(§5.4). A peer that stalls across many chunks is a candidate for the p2p blocklist
(`p2p.Blocklister.Blocklist(addr, dur, reason)`), but **the doc does not require a stall-budget
blocklist** — that was a `swip-25d` addition. If you add one, make it a clearly-labelled
operational safeguard with a cooldown, not part of the core correctness story, and keep it behind
a tunable in `Options` (default off or generous). Don't let it gold-plate the PR.

---

## 5. Files to touch

| Path | Change |
|---|---|
| `pkg/pullsync/pullsync.go` | Split `Sync` → `Offer` + `Fetch` (shared verify/store helpers); keep handler + wire identical; keep `protocolVersion = "1.4.0"`. |
| `pkg/pullsync/mock/pullsync.go` | Implement `Offer`/`Fetch`; keep `Option` pattern + interface check. |
| `pkg/pullsync/pullsync_test.go` | Rework the `Sync`-based tests to `Offer`/`Fetch`; keep `synctest`+`streamtest`+`mock.NewReserve` style. |
| `pkg/pullsync/metrics.go` | Keep counters; rename/add as the new flow needs (e.g. split offered vs fetched). |
| **`pkg/puller/internal/pullstate/pullstate.go`** (new) | The pure state machine — the 1:1 translation of `PullSyncerE.tla` (§4.2–§4.4): variables, `Config` knobs, `Want`/`Deliver`/`Stall`/`NewChunk`, `Claimable`/`prioOK`, `Step()`→`Op`s. No I/O, no goroutines, no locks. Put the TLA→Go mapping table in the package doc comment. |
| `pkg/puller/internal/pullstate/pullstate_test.go` (new) | Unit-test the state machine **against the same matrix as `optimal-testbed/run.sh`** (the ablation parity table, §8). |
| `pkg/puller/puller.go` | Replace per-peer sessions with the single-goroutine I/O shell that drives `pullstate` (§4.5). |
| `pkg/puller/metrics.go` | Add: deliveries (should ≈ missing-chunk count), dedup-suppressed wants, failovers/exclusions, per-peer load skew, and a **`conflict`** counter (must stay 0 — the `ConflictFree` tripwire, §4.3). These metrics *are* the O3/O5 evidence. |
| `pkg/puller/export_test.go` | Export the test seams you need (inspect in-flight/holders/load, set the tick interval). Mirror the existing `PeerIntervalKey` export idiom. |
| `pkg/puller/puller_test.go` | New integration tests per §8. |
| `pkg/node/node.go` | Only if the `puller.New`/`pullsync.New` signatures change. Keep them identical if you can (the call sites are `node.go:1064` and `node.go:1155`). |

**Do not touch:** `pkg/pullsync/pb/*`, `pkg/storer/*`, `pkg/topology/*`, anything postage.

---

## 6. House conventions to follow (bee style)

Distilled from `CONTRIBUTING.md`, `CODINGSTYLE.md`, `.golangci.yml`, and the surrounding code.
Match the files you edit; when in doubt, copy the idiom from `puller.go`/`pullsync.go`.

- **License header** on every new file (note the year is `2026`):
  ```go
  // Copyright 2026 The Swarm Authors. All rights reserved.
  // Use of this source code is governed by a BSD-style
  // license that can be found in the LICENSE file.
  ```
- **Logger:** `const loggerName = "puller"`; `logger.WithName(loggerName).Register()`; structured
  key/value pairs: `p.logger.Debug("msg", "peer_address", addr, "bin", bin)`. Levels:
  Error/Warning/Info/Debug.
- **Metrics:** the `metrics` struct + `newMetrics()` + `func (p *Puller) Metrics() []prometheus.Collector`
  pattern, `Namespace`/`Subsystem`/`Name`/`Help`, registered via
  `m.PrometheusCollectorsFromFields(...)`. Mirror `pkg/puller/metrics.go`.
- **Errors:** sentinel `var ErrFoo = errors.New(...)`; wrap with `fmt.Errorf("doing x: %w", err)`;
  aggregate with `errors.Join`; classify with `errors.Is`. Keep the existing `countErrors` helper
  idea if you still join many per-chunk errors.
- **Concurrency:** single control goroutine owns mutable scheduling state (no lock); workers get a
  child `context.WithCancel`; `sync.WaitGroup` + bounded `Close` timeout; report results on an
  unbuffered channel; a `quit`/`done` channel so a worker never blocks on send past shutdown.
  Guard *only* the genuinely-shared maps (statestore access, cursor cache) with a mutex.
- **Options:** `Options` struct with zero-value defaults resolved in `New` (`if bins == 0 { bins =
  swarm.MaxBins }`).
- **Interface compliance:** `var _ pullsync.Interface = (*PullSyncMock)(nil)` and
  `var _ Interface = (*Puller)(nil)`.
- **Linters** (`.golangci.yml`): `paralleltest` (mark tests `t.Parallel()`), `prealloc`,
  `errorlint`, `noctx`, `gochecknoinits` (no `init()` except the `// nolint:gochecknoinits` test
  pattern already used), `goheader`, `prealloc`, `unconvert`. Run `make lint` and `make test`
  (or `make test-race`) before declaring done. `make format FOLDER=pkg/puller` runs gofumpt+gci.
- **Enums start at one / `String()`:** see the `swip-25d` `SyncState`/`OpKind` for the idiom if you
  add enums.

---

## 7. Acceptance criteria (what "done" means)

Functional (map to the doc's objectives and §9 machine-checked properties):

1. **O3 / exactly-once (`ConflictFree`).** For a missing chunk offered by all `k` peers, exactly
   **one** delivery occurs. Measured: total deliveries ≈ number of distinct missing chunks, not
   `k×`. This is *the* headline result. Its sharpest form is
   `TestOptimal_ConcurrentWantsDedupToOneFetch`: two holders offering the same chunk yield a
   single want in flight (atomic check-and-mark, §2 mechanism 2) — run it under `-race`.
2. **O1 / completeness (`Completeness`).** Every chunk held by ≥1 honest neighbour is eventually
   fetched, including under an omitting/stalling peer and under partial holdings (a chunk on a
   single holder).
3. **O6 / failover-with-exclude.** A stalling holder is excluded per chunk and the fetch succeeds
   from another holder; a single source that withholds does **not** stall completeness.
4. **Freshness (LIVE).** A chunk arriving after the start-cursor is eventually fetched.
5. **O5 / load.** Across a HIST drain with `M ≫ k` fully-replicated chunks, per-peer serve counts
   are within ~1 of `M/k` (free-choice balance, §5.3).
6. **O2 / order.** Deeper bins are scheduled before shallower ones.
7. **No regressions:** radius increase/decrease, epoch reset, peer disconnect/gone, interval
   persistence/resume all behave as the master tests assert (port those tests).
8. `go build ./...`, `make lint`, `make test` (and `-race`) green; `goleak` clean (the
   `main_test.go` `TestMain` stays).

Non-functional: no wire/`.proto` change; statestore interval format unchanged (no migration);
`puller.New`/`pullsync.New` signatures unchanged if at all possible.

---

## 8. Test strategy & failing scaffold

Two layers. The first is the one that makes "runs as in the TLA models" concrete.

### 8.1 State-machine parity with the TLA matrix (the primary correctness evidence)

Unit-test the pure `pullstate` machine (§4.2) against **the same config matrix as
`optimal-testbed/run.sh`** — same knobs, same expected outcomes. This is the Go analogue of the
model checker: drive the machine through interleavings, assert the invariant
(`conflict==false`) holds (positives) or breaks (ablations), and that the deficit reaches zero
(`Completeness`) or doesn't (the failover/single-source ablations), and that LIVE arrivals are
delivered (`Freshness`) or aren't (`MC_no_live`). Because the machine is pure and tiny, the test
can either enumerate interleavings exhaustively for `k=2,3` (a hand-rolled BFS over enabled
actions — the closest thing to TLC in Go) or drive randomized schedules with a seed.

| Go test case | knob delta from all-on | expect | mirrors |
|---|---|---|---|
| `base` | — (full repl, honest) | `conflict==false`, deficit→0 | `MC_base` |
| `partial` | one chunk on a single holder | same | `MC_partial` |
| `omission` | one holder never delivers (→`Stall`) | same (repaired by failover) | `MC_omission` |
| `vicinity` | `Priority=true` | same (order is correctness-neutral) | `MC_vicinity` |
| `live` | a `NewChunk` after init | same + `Freshness` | `MC_live` |
| `nodedup` | `Dedup=false` | **`conflict` latches** | `MC_nodedup` |
| `nofailover` | `Failover=false` + omitter | **deficit stuck >0** | `MC_nofailover` |
| `noexclude` | `Exclude=false` + omitter | **deficit stuck >0** (re-grab livelock) | `MC_noexclude` |
| `single_omission` | `SingleSource=true`, assigned=omitter | **deficit stuck >0** | `MC_single_omission` |
| `single_partial` | `SingleSource=true`, assigned lacks it | **deficit stuck >0** | `MC_single_partial` |
| `no_live` | `EnableLive=false` + a `NewChunk` | **`Freshness` fails** | `MC_no_live` |

This table belongs in `pkg/puller/internal/pullstate/pullstate_test.go`. A scaffold for it is at
`pkg/puller/internal/pullstate/pullstate_scaffold_test.go` (build-tagged). **If a knob is off and
the property does *not* break, the translation is wrong — that is the whole point of keeping the
ablations.**

### 8.2 Integration scaffold (puller + pullsync over the mocks)

The build-tagged scaffolds:
- `pkg/puller/optimal_scaffold_test.go`
- `pkg/pullsync/optimal_scaffold_test.go`

All scaffolds are guarded by `//go:build pullsync_optimal_scaffold` so the **master build stays
green** while the API they reference does not yet exist. As you implement, flip them on
(`go test -tags pullsync_optimal_scaffold ./pkg/puller/...`) and, once the new API is the real one,
delete the tag so they run by default. Each test maps to a numbered acceptance criterion in §7 and
uses the existing infra (`kadMock`, `mockps`, `resMock`, `leveldb` statestore, `spinlock`,
`synctest`, `streamtest`). They are deliberately skeletal — the scenario names and table cases are
the spec. **Do not weaken a test to make it pass; fix the code.**

---

## 9. Prior art — the `swip-25d-pullsyncer` branch (reference for the I/O shell ONLY)

There is an existing branch, `swip-25d-pullsyncer`, that implements an **earlier, coarser**
design (SWIP-25D): a two-phase **bin-level** scheduler — Phase 1 syncs each bin from the single
nearest peer, Phase 2 fans out to all peers on completion/stall. `pullsync.go` is untouched there.

**The doc explicitly rejects bin-level delegation as "too coarse" (§5.2):** it forfeits
within-bin load balancing and per-chunk failover (a stall fails the whole bin). So **do not copy
its scheduling core** — this PR is per-*chunk*, that branch is per-*bin*.

What *is* worth studying there (good bee craftsmanship to mirror):
- the **single non-blocking control goroutine** shell in `pkg/puller/puller.go` (worker
  generations, `results` channel, `quit`-guarded `report`, bounded `Close`) — the I/O architecture
  is exactly what §4 wants;
- the **pure, lock-free scheduler in `pkg/puller/internal/scheduler/`** unit-tested in isolation —
  the pattern (not the policy) is the right call for testability;
- the per-peer **cursor cache** fetched off the control path (`peerCursorsFor`);
- the metrics/log/options idioms.

Read it for *how to wire I/O cleanly in this codebase*; ignore it for *what to schedule*.

---

## 10. Risks & things to verify against source (don't take this brief on faith)

- **Stream lifecycle on `Offer` close.** Confirm against `handler` how to end the stream after
  reading the Offer without an error log (empty `Want` vs `Reset`). Pick the quiet path.
- **`Fetch` re-offer drift.** The fresh offer in `Fetch` may differ from the one `Offer` saw
  (new arrivals, evictions). Match `want` by triple and tolerate misses (a wanted ref no longer
  offered → treat as a stall for that chunk → reschedule). Test it.
- **Interval advance** (§4.4) — the one genuinely new correctness subtlety. Single highest-value
  test.
- **`BinID` is peer-local.** Never compare BinIDs across peers; dedup on address only.
- **Backpressure / rate limit.** Keep the `~1000 chunks/s` puller limiter (`maxChunksPerSecond`)
  and the server-side per-peer limiter. Don't let the offer-gathering fan-out blow past them.
- **`k ∈ [2,8]`** is small; O(k) scans per chunk are fine. Don't over-engineer the data
  structures.
- **Supply failure is not pull's bug.** If every holder of a chunk is excluded/absent, the chunk
  stays missing — that is an availability failure (§6.1), not a completeness bug. Log it; don't
  spin.

If anything here contradicts what you read in the doc or the code, **stop and flag it** — the
author pushes back on claims taken from summaries rather than read from source.
