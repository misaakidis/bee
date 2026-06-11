// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build pullsync_optimal_scaffold

// Failing test scaffold for the pull-sync optimal design per-chunk scheduler
// (see PULLSYNC_OPTIMAL_IMPLEMENTATION.md §4 and §8). Build-tagged so the default
// build/test stays green until the scheduler and the Offer/Fetch-aware pullsync
// mock exist.
//
// To work on it:   go test -tags pullsync_optimal_scaffold ./pkg/puller/...
// Once the new API is real, DELETE the build tag so these run by default.
//
// Each test maps to a numbered acceptance criterion in the brief's §7. They are
// written against the INTENDED interfaces and a few not-yet-existing test seams,
// flagged with `INTENDED:` comments. Implement those seams as part of the PR:
//
//   - pullsync mock (pkg/pullsync/mock): record Offer/Fetch and let a test script
//       * which peer offers which chunks per bin   -> mockps.WithOffer(peer, bin, refs...)
//       * which peer stalls (Fetch returns error)  -> mockps.WithFetchStall(peer)
//       * read back what was actually fetched       -> mockps.Fetched() []FetchRecord
//         where FetchRecord{Peer swarm.Address; Bin uint8; Refs []pullsync.ChunkRef; Order int}
//   - puller export_test.go: whatever inspection seam the assertions need.
//
// Build chunk refs with the helper genRefs below. DO NOT weaken a test to make it
// pass — fix the scheduler.

package puller_test

import (
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// genRefs makes n distinct chunk refs in the given bin (addresses at proximity `bin`
// to base), each with a dummy batchID/stampHash. INTENDED: return []pullsync.ChunkRef.
func genRefsAt(t *testing.T, base swarm.Address, bin uint8, n int) []swarm.Address {
	t.Helper()
	out := make([]swarm.Address, n)
	for i := range out {
		out[i] = swarm.RandAddressAt(t, base, int(bin))
	}
	return out
}

// §7.1 — EXACTLY-ONCE (ConflictFree, O3). The headline result.
// Setup: a fully-replicated bin — every one of k peers offers the SAME m chunks,
// none held locally. Expect: exactly m Fetch deliveries total across all peers
// (one per distinct address), NOT k*m. This is the up-to-kx bandwidth saving.
func TestOptimal_ExactlyOncePerChunk(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: implement per-chunk dedup (brief §4.3 step 2, §5.2) then assert: " +
		"sum of fetched refs across peers == number of distinct missing addresses (not k * m)")

	// SKETCH:
	// base := swarm.RandAddress(t)
	// peers := k addresses at PO==bin
	// refs  := genRefsAt(t, base, bin, m)
	// each peer offers the same refs (mockps.WithOffer(peer, bin, refs...))
	// run puller; wait for convergence
	// got := flattenFetched(ps.Fetched())
	// assertEachAddressFetchedExactlyOnce(t, got, refs)
}

// §5.2 / §9 (MC_nonatomic) — TWO CONCURRENT WANTS for the same chunk must dedup to ONE
// Fetch. This is the atomicity result and the sharpest statement of what chunk-level dedup
// means: when two holders A and B both offer the same missing chunk c, the scheduler may
// only ever have ONE want for c in flight at a time. The shared in-flight set is consulted
// AND marked as a single indivisible step:
//
//	if c.Address not in inFlight {        // <-- check
//	    inFlight[c.Address] = chosenPeer  // <-- mark   (same critical section as the check)
//	    fetch c from chosenPeer
//	}
//
// In this design the single control goroutine owns inFlight, so check-and-mark is atomic by
// construction (brief §4). If it were split into a Decide (test want==empty) and a later
// Commit (mark), two would-be wants for c can both pass the test before either marks — both
// then fetch c, c is delivered twice, and ConflictFree breaks. That is exactly the doc's
// §9 MC_nonatomic counterexample. The second holder is the failover fallback (§5.1/§5.4),
// used only if the first stalls — never a concurrent second delivery.
//
// Expect: across A and B, c's address is fetched EXACTLY ONCE; the other holder is not
// fetched for c unless the first stalls. Run with -race to surface any non-atomic access.
func TestOptimal_ConcurrentWantsDedupToOneFetch(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: two holders A,B both offer the same chunk c; assert c's address is fetched " +
		"exactly once (one want in flight at a time), the other holder is the unused fallback, and " +
		"-race is clean. Splitting check-and-mark (MC_nonatomic) must double-fetch — guard against it.")

	// SKETCH:
	// base := swarm.RandAddress(t)
	// a := swarm.RandAddressAt(t, base, bin); b := swarm.RandAddressAt(t, base, bin)
	// c := genRefsAt(t, base, bin, 1)[0]
	// both A and B offer c (mockps.WithOffer(a, bin, c), mockps.WithOffer(b, bin, c))
	// drive both peers' Offers to arrive together; run puller; wait for convergence
	// fetched := flattenFetched(ps.Fetched())
	// assertEachAddressFetchedExactlyOnce(t, fetched, []swarm.Address{c})  // NOT twice
	// // and: only one of {A,B} fetched c (the other is the idle fallback)
}

// §7.2 — COMPLETENESS under partial holdings (O1, O6b). A chunk on a SINGLE honest
// holder must still be fetched. Expect: every offered address is fetched exactly once,
// the single-holder chunk from its only holder.
func TestOptimal_CompletenessPartialHoldings(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: one chunk offered by only one peer, the rest by all; assert all fetched once")
}

// §7.3 — FAILOVER-WITH-EXCLUDE (O1, O6 under omission/stall, §5.4). One holder stalls
// (Fetch errors) on the chunks assigned to it. Expect: those chunks are re-fetched from
// another holder, the staller is excluded for them (never re-selected for the same chunk),
// and completeness still holds. Without exclude this livelocks (doc §9 MC_noexclude).
func TestOptimal_FailoverExcludesStaller(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: peer A stalls; chunks it was assigned must be fetched from peer B; " +
		"assert A is never re-fetched for those addresses and completeness holds")
}

// §7.3 — SINGLE-SOURCE FAILS (multi-source is necessary, §5.1; doc §9 MC_single_*).
// Negative control: if a chunk has only one candidate holder and that holder withholds,
// the chunk cannot complete. Verifies the scheduler does not pretend success — it should
// surface/retry, not silently advance the interval past the missing chunk (brief §4.4).
func TestOptimal_SingleSourceOmissionDoesNotComplete(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: lone holder withholds one chunk; assert that chunk stays missing and " +
		"its (peer,bin) interval is NOT advanced past it (O1 / brief §4.4)")
}

// §7.4 — FRESHNESS / LIVE (§5.6; doc §9 MC_no_live). A chunk that arrives AFTER the
// start-cursor (offered only on a later Offer with start>cursor) must eventually be fetched.
func TestOptimal_LiveArrivalIsFetched(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: post-cursor arrival appears on a later Offer; assert it is fetched")
}

// §7.5 — LOAD BALANCE (O5, §5.3). HIST drain, M >> k, fully replicated. Expect: per-peer
// fetch counts within ~1 of M/k (free-choice list scheduling). Asserts the least-loaded
// routing actually spreads load rather than hammering the nearest peer.
func TestOptimal_LoadBalancedAcrossHolders(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: M=k*q chunks offered by all k peers; assert max per-peer fetched - min <= 1")
}

// §7.6 — DEEPEST-FIRST ORDER (O2, §5.5). Chunks offered across several bins; expect the
// deeper (higher-PO) bins to be fetched before shallower ones. Uses the recorded Fetch order.
func TestOptimal_DeepestBinsFetchedFirst(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: offers in bins r..maxBins; assert fetch order is non-increasing in bin")
}

// §4.4 — INTERVAL ADVANCE after cross-peer fetch (the genuinely-new correctness subtlety).
// A chunk peer A offered is fetched from peer B (dedup). Expect: A's (peer,bin) interval still
// advances to A's offer Topmost (everything A offered is now held), so A is not re-queried for
// it; but if a chunk A offered is still missing at round end, A's interval stops at the
// contiguous held prefix.
func TestOptimal_IntervalAdvanceAfterCrossPeerFetch(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: fetch A's chunk from B; assert A's interval advances to Topmost when all " +
		"held, and stops short of a still-missing offered chunk")
}

// §7.7 — REGRESSIONS. Port the master puller tests for radius increase/decrease, epoch reset,
// peer disconnect/gone, and interval persistence/resume. They must pass against the new
// scheduler. (Move them out of the scaffold once ported; listed here as a checklist.)
func TestOptimal_RegressionChecklist(t *testing.T) {
	t.Skip("checklist: port TestRadiusDecreaseNeighbor, TestRadiusDecreaseNonNeighbor, " +
		"TestRadiusIncrease, TestEpochReset, TestPeerDisconnected, TestPeerGone, TestSyncIntervals")
}
