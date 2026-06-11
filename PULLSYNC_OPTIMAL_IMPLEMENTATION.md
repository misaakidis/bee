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

## 4. The per-chunk scheduler (`pkg/puller`)

Keep the master puller's **structural lesson** intact: a **single control goroutine** owns all
scheduling state and never blocks on a network call. (This is also the lesson the legacy puller
violated and the cause of the radius-decrease freeze.) Workers do the I/O off the control path
and report back over a channel. The scheduler state — holder map, in-flight set, exclusions,
per-peer load — is mutated **only** by that one goroutine, which makes the §5.2 atomic
check-and-mark free.

### 4.1 State (owned by the control goroutine, no locks)

```text
radius        uint8
peers         set of connected eligible peers, with proximity(base,peer)
perBin        for each bin >= radius:
                offers   map[peerKey]offerState   // last offer from each peer (refs + topmost + per-peer start)
                holders  map[address][]peerKey     // who offered each still-missing chunk
inFlight      map[address]peerKey                  // §5.2 shared dedup set (the claim set)
excluded      map[address]set[peerKey]             // §5.4 per-chunk failover log
load          map[peerKey]int                      // §5.3 outstanding assigned fetches per peer
intervals     persisted per (peer,bin) high-water  // statestore, as today
```

- **HIST vs LIVE** is, as today, the cursor boundary: BinIDs ≤ the peer's cursor-at-start are
  HIST, above are LIVE. Reuse `intervalstore.Intervals` and the `sync_interval_%03d_%s`
  statestore keys (`peerIntervalKey`) **unchanged** — the on-disk format must stay compatible
  (no migration). Keep `IntervalPrefix = "sync_interval"`.

### 4.2 Control loop (mirror master's `manage`)

Events: topology change (`SubscribeTopologyChange`), radius poll/change, worker result, and a
periodic scheduling tick. On each event, update state and (re)compute assignments, then spawn/cancel
workers. Never call `Offer`/`Fetch`/`GetCursors` on the control goroutine — push them to workers.

### 4.3 The per-chunk algorithm (§5.6 — this is the heart of the PR)

For a neighbourhood, per scheduling round:

1. **Discovery.** For each eligible peer `p` (proximity ≥ bin, bin ≥ radius), a worker calls
   `Offer(p, bin, start_p)` where `start_p` is `p`'s persisted high-water for that bin. Record
   `offers[p]` and, for each offered ref whose triple is **not** locally held
   (`ReserveHas==false`), add `p` to `holders[addr]`.
   - `start_p` is **per-peer** because BinIDs are assigned independently by each peer (a chunk is
     BinID 5 at one peer, 9 at another). Dedup by **address** is still global and correct; only
     the resume bookkeeping is per-peer. **This is the subtle invariant — see §4.4.**
2. **Schedule, deepest bins first.** Iterate bins high→low. For each still-missing chunk `c` in a
   bin (any order within the bin):
   - if `c`'s address `∈ inFlight`: **skip** (already claimed).
   - `cands = holders[c] \ excluded[c]`; if empty: leave for a later round (supply may appear, or
     the chunk is unfetchable — an availability failure outside pull's remit, §6.1).
   - choose `p* = argmin load[p]` over `cands`, ties broken by highest proximity (`swarm.Proximity`).
     The XOR/`swarm.DistanceCmp` tiebreak balances load network-wide (it splits a tie to different
     peers for different pivots — see §5.3 / the `swip-25d` `assign` for the idiom, but note that
     code routes *whole bins*, here you route *one chunk*).
   - **atomically** set `inFlight[addr] = p*`, `load[p*]++`, and enqueue `c` onto `p*`'s want set.
3. **Fetch.** For each peer with a non-empty want set, a worker calls `Fetch(p, bin, start_p,
   want)`. On the result, the worker reports back; the control goroutine:
   - **delivered** chunk `c`: clear `inFlight[addr]`, `load[p]--`, drop `c` from `holders`.
   - **stall / under-delivery / error** for `c`: clear `inFlight[addr]`, `load[p]--`, add `p` to
     `excluded[c]` (§5.4 — **permanent for that chunk**, so the staller cannot re-grab it), and
     re-schedule `c` in the next round (it will pick another holder). Bound stalls per chunk to
     `≤ k`.
4. **Advance intervals.** See §4.4.
5. **LIVE.** Once a bin's HIST is drained for a peer (its interval reaches the start-cursor), keep
   a live subscription open: each newly-arrived chunk enters the same per-chunk loop (one round).
   A node that only drains HIST never converges on post-cursor arrivals — freshness fails (doc
   §5.6 / `MC_no_live` ablation).

`pullsync.Fetch` returns an aggregate count, not per-chunk outcomes. The cleanest way to get
per-chunk failover is to **fetch one peer's assigned subset and treat a `Fetch` error as a stall
for that whole subset** (exclude `p` for each `c` in the subset, reschedule them). That preserves
the gate-critical guarantee (each `c` retried on another holder) without per-chunk wire feedback.
If you want finer granularity, have `Fetch` return which refs it actually delivered — that is a
legitimate Go-API addition (still no wire change, since deliveries already carry their address).
**Document whichever you choose and cover it in tests.**

### 4.4 The interval-advance invariant (correctness-critical — get this right)

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

### 4.5 Radius changes (keep master's behaviour)

- **Decrease** (bins re-enter the reserve): reset the affected bins' intervals so evicted/ignored
  chunks resync (master's `resetIntervals`). Re-tile per §4 of the doc.
- **Increase**: stop syncing bins that left the reserve.
- Reuse `storer.RadiusChecker.StorageRadius()`; react on topology change and a poll tick, as today.

### 4.6 Misbehaviour / blocklist

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
| `pkg/puller/puller.go` | Replace per-peer sessions with the single-goroutine per-chunk scheduler (§4). |
| `pkg/puller/metrics.go` | Add: deliveries (should ≈ missing-chunk count), dedup-suppressed wants, failovers/exclusions, per-peer load skew. These metrics *are* the O3/O5 evidence. |
| `pkg/puller/export_test.go` | Export the test seams you need (e.g. a way to inspect in-flight/holders/load, set the tick interval). Mirror the existing `PeerIntervalKey` export idiom. |
| `pkg/puller/puller_test.go` | New tests per §8. |
| (maybe) `pkg/puller/internal/...` | If the scheduler grows, factor a pure, lock-free scheduler type into `internal/` and unit-test it in isolation (the `swip-25d` branch did this and it paid off — see §9). The scheduler being pure (no I/O) is what makes it exhaustively testable. |
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

## 8. Failing test scaffold

The scaffold lives in:
- `pkg/puller/optimal_scaffold_test.go`
- `pkg/pullsync/optimal_scaffold_test.go`

Both are guarded by the build tag `//go:build pullsync_optimal_scaffold` so the **master build
stays green** while the API they reference does not yet exist. As you implement, flip them on
(`go test -tags pullsync_optimal_scaffold ./pkg/puller/...`) and, once the new API is the real
one, delete the tag so they run by default. Each test maps to a numbered acceptance criterion in
§7 and is written against the **intended** `Offer`/`Fetch` API and the existing mock infra
(`kadMock`, `mockps`, `resMock`, `leveldb` statestore, `spinlock`, `synctest`, `streamtest`). They
are deliberately skeletal — fill the assertions as the behaviour solidifies, but the table cases
and the scenario names are the spec. **Do not weaken a test to make it pass; fix the code.**

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
