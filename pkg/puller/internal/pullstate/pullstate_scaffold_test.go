// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build pullsync_optimal_scaffold

// Failing test scaffold for the per-chunk state machine — the 1:1 translation of
// optimal-testbed/PullSyncerE.tla (see PULLSYNC_OPTIMAL_IMPLEMENTATION.md §4 and §8.1).
// Build-tagged so the default build stays green until pkg/puller/internal/pullstate exists.
//
//	go test -tags pullsync_optimal_scaffold ./pkg/puller/internal/pullstate/...
//
// This is the Go analogue of the TLA model checker: it drives the PURE state machine
// through interleavings of the four actions (Want/Deliver/Stall/NewChunk) and asserts the
// SAME outcomes as optimal-testbed/run.sh — knob for knob. If a knob is off and the
// property does NOT break, the translation is wrong. That is the whole point of the matrix.
//
// INTENDED package API (implement in pullstate.go). Identity is the (addr,batchID,stampHash)
// triple — model `Chunk` == triple (brief §2); `c` below is a triple key.
//
//	type Config struct{ Dedup, Failover, Exclude, ResetOnExhaust, SingleSource, Priority, EnableLive bool }
//	func New(cfg Config, topo Topology) *Machine        // topo: holders[c], prio[c], assign[c]
//	func (m *Machine) NewChunk(c Triple)                 // LIVE arrival -> arrived
//	func (m *Machine) Enabled() []Op                     // enabled Want(c,p), the Next relation
//	func (m *Machine) Want(c Triple, p swarm.Address) bool   // fire Want if Claimable
//	func (m *Machine) Deliver(c Triple, p swarm.Address)     // honest delivery (ndeliv++)
//	func (m *Machine) Stall(c Triple, p swarm.Address)       // ANY non-delivery (Byz stall OR honest misfire)
//	func (m *Machine) Lose(c Triple, p swarm.Address)        // churn: holder drops c (supply preserved)
//	func (m *Machine) Gain(c Triple, p swarm.Address)        // churn: holder (re)acquires c
//	// observers (the run.sh invariants/properties):
//	func (m *Machine) Conflict() bool      // ConflictFree tripwire (must stay false)
//	func (m *Machine) Ndeliv() int         // DeliveryFloor: must equal len(Got set)
//	func (m *Machine) Deficit() int        // |arrived \ got| (0 == Completeness)
//	func (m *Machine) SupplyOK() bool       // SupplyInv: every chunk still on >=1 honest holder
//	func (m *Machine) Got(c Triple) bool
//
// Stall folds the model's ByzStall AND SpuriousTimeout (the impl can't tell them apart — brief §4.3).
// ResetExcluded fires internally when failed[c] covers every current holder (ResetOnExhaust).
// All-on production config: Dedup=Failover=Exclude=ResetOnExhaust=EnableLive=Priority=true,
// SingleSource=false.

package pullstate_test

import (
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// matrixCase mirrors one row of optimal-testbed/run.sh.
type matrixCase struct {
	name string
	// knob deltas from the all-on production config
	dedup, failover, exclude, resetOnExhaust, singleSource, priority, enableLive bool
	// scenario / located-idealisation budgets
	omitter       bool // a Byzantine holder never delivers (-> Stall)
	singleHolder  bool // one chunk on exactly one holder (partial / worst case)
	live          bool // a chunk arrives post-cutoff (NewChunk)
	timeoutBudget int  // spurious timeouts on honest peers (misfire)
	churnBudget   int  // Lose/Gain events
	peers         int  // tile size k (0 => 3)
	// expectations
	wantConflict bool // ConflictFree must break?  (nodedup; nonatomic lives in §8.2)
	wantStuck    bool // deficit must stay > 0?     (nofailover, noexclude, noreset, single_*)
	wantUnfresh  bool // a LIVE chunk never delivered? (no_live)
}

// The matrix — same rows as optimal-testbed/run.sh (positives + ablations). Keep it in lockstep
// with that file: if a row is added/changed there, change it here.
func matrix() []matrixCase {
	all := matrixCase{dedup: true, failover: true, exclude: true, resetOnExhaust: true, singleSource: false, priority: true, enableLive: true}
	with := func(name string, f func(*matrixCase)) matrixCase {
		c := all
		c.name = name
		f(&c)
		return c
	}
	return []matrixCase{
		// positives — invariants hold, deficit reaches zero
		with("base", func(c *matrixCase) {}),
		with("partial", func(c *matrixCase) { c.singleHolder = true }),
		with("omission", func(c *matrixCase) { c.omitter = true }),
		with("vicinity", func(c *matrixCase) { c.priority = true }),
		with("live", func(c *matrixCase) { c.live = true }),
		// positives — relaxed assumptions (idealisations as knobs)
		with("timeout", func(c *matrixCase) { c.singleHolder = true; c.timeoutBudget = 2 }),
		with("churn", func(c *matrixCase) { c.omitter = true; c.churnBudget = 2 }),
		with("storm", func(c *matrixCase) {
			c.peers = 4
			c.omitter = true
			c.singleHolder = true
			c.live = true
			c.timeoutBudget = 1
			c.churnBudget = 1
		}),
		with("scale", func(c *matrixCase) { c.peers = 6; c.omitter = true }), // two Byzantine in run.sh
		// ablations — the named property must break
		with("nodedup", func(c *matrixCase) { c.dedup = false; c.wantConflict = true }),
		with("nofailover", func(c *matrixCase) { c.failover = false; c.omitter = true; c.wantStuck = true }),
		with("noexclude", func(c *matrixCase) { c.exclude = false; c.omitter = true; c.wantStuck = true }),
		with("noreset", func(c *matrixCase) {
			c.resetOnExhaust = false
			c.singleHolder = true
			c.timeoutBudget = 1
			c.wantStuck = true
		}),
		with("single_omission", func(c *matrixCase) { c.singleSource = true; c.omitter = true; c.wantStuck = true }),
		with("single_partial", func(c *matrixCase) { c.singleSource = true; c.singleHolder = true; c.wantStuck = true }),
		with("no_live", func(c *matrixCase) { c.enableLive = false; c.live = true; c.wantUnfresh = true }),
	}
}

// TestStateMachine_AblationMatrix is the Go model-checker: each row drives the pure machine to
// quiescence (or a bounded number of steps) and asserts the same verdict as run.sh.
func TestStateMachine_AblationMatrix(t *testing.T) {
	t.Parallel()
	for _, tc := range matrix() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			t.Skip("scaffold: implement pullstate, then drive this case and assert: " +
				"wantConflict -> m.Conflict()==true; wantStuck -> m.Deficit() stays > 0; " +
				"wantUnfresh -> the LIVE chunk never reaches Got; otherwise Conflict()==false && Deficit()==0")

			// SKETCH (k=3, chunks=3, full replication unless singleHolder):
			// topo := buildTopology(t, tc)              // holders[c], prio[c], assign[c]
			// m := pullstate.New(configOf(tc), topo)
			// honest := func(p) bool { return !(tc.omitter && p == theOmitter) }
			// driveToQuiescence(t, m, honest, tc.live)  // see driver below
			// switch {
			// case tc.wantConflict: assertTrue(t, m.Conflict())
			// case tc.wantStuck:    assertGreater(t, m.Deficit(), 0)
			// case tc.wantUnfresh:  assertFalse(t, m.Got(liveChunk))
			// default:              assertFalse(t, m.Conflict()); assertEqual(t, m.Deficit(), 0)
			// }
		})
	}
}

// driveToQuiescence is the Go stand-in for TLC's exploration: repeatedly fire an enabled Want and
// resolve it (Deliver if the chosen holder is honest and holds c, else Stall), inject the LIVE
// arrival once, and stop when no action is enabled or a step budget is hit. For small k a BFS over
// Enabled() is closer to exhaustive; a seeded random schedule is enough to surface the ablations.
func driveToQuiescence(t *testing.T /* m *pullstate.Machine, honest func(swarm.Address) bool, */, live bool) {
	t.Helper()
	t.Skip("scaffold: implement the action-interleaving driver (see comment)")
	_ = live
}

// TestStateMachine_ConflictLatchesOnDoubleDeliver pins the ConflictFree semantics directly: a
// second Deliver of an already-got chunk latches conflict (this is how nodedup/nonatomic fail).
func TestStateMachine_ConflictLatchesOnDoubleDeliver(t *testing.T) {
	t.Parallel()
	t.Skip("scaffold: with Dedup off, fire Want(c,a) and Want(c,b), Deliver(c,a), Deliver(c,b); " +
		"assert Conflict()==true. With Dedup on, the second Want must not be Claimable.")
	_ = swarm.ZeroAddress
}
