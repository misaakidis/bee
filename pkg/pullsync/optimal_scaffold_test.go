// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build pullsync_optimal_scaffold

// Failing test scaffold for the pull-sync optimal design (see
// PULLSYNC_OPTIMAL_IMPLEMENTATION.md §3 and §8). Build-tagged so the default
// build/test stays green until the Offer/Fetch decomposition exists.
//
// To work on it:   go test -tags pullsync_optimal_scaffold ./pkg/pullsync/...
// Once Offer/Fetch is the real API, DELETE the build tag so these run by default.
//
// These tests pin the protocol decomposition: Offer discovers without fetching;
// Fetch downloads only the requested subset over an unchanged wire. They reuse the
// existing harness in pullsync_test.go (newPullSync, someChunks, haveChunks,
// results/chunks/addrs, synctest+streamtest+mock.NewReserve) — keep that style.

package pullsync_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/pullsync"
	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// §7.1 (precondition): Offer returns the server's chunks for the range and the
// topmost BinID, and fetches NOTHING — the client store must see zero puts.
func TestOffer_DiscoversWithoutFetching(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			wantTopmost      = uint64(4)
			server, _        = newPullSync(t, nil, 5, mock.WithSubscribeResp(results, nil), mock.WithChunks(chunks...))
			recorder         = streamtest.New(streamtest.WithProtocols(server.Protocol()))
			client, clientDB = newPullSync(t, recorder, 0)
		)

		// INTENDED API: Offer(ctx, peer, bin, start) ([]pullsync.ChunkRef, topmost uint64, err error)
		refs, topmost, err := client.Offer(context.Background(), swarm.ZeroAddress, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if topmost != wantTopmost {
			t.Fatalf("offer topmost: got %d want %d", topmost, wantTopmost)
		}
		if len(refs) != len(chunks) {
			t.Fatalf("offered refs: got %d want %d", len(refs), len(chunks))
		}
		// the offer must carry the triple + peer-local BinID, address-comparable across peers
		for _, r := range refs {
			if r.Address.IsZero() {
				t.Fatal("offered ref has zero address")
			}
			if len(r.BatchID) == 0 || len(r.StampHash) == 0 {
				t.Fatalf("offered ref %s missing batchID/stampHash", r.Address)
			}
		}
		// Offer fetches nothing.
		if clientDB.PutCalls() != 0 {
			t.Fatalf("Offer must not store: got %d puts", clientDB.PutCalls())
		}
	})
}

// §7.1: Fetch downloads ONLY the requested subset (one delivery per requested ref),
// verifies and stores it. Wire is unchanged (Get→Offer→Want→Delivery under the hood).
func TestFetch_DownloadsOnlyRequestedSubset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			server, _        = newPullSync(t, nil, 5, mock.WithSubscribeResp(results, nil), mock.WithChunks(chunks...))
			recorder         = streamtest.New(streamtest.WithProtocols(server.Protocol()))
			client, clientDB = newPullSync(t, recorder, 0)
		)

		refs, _, err := client.Offer(context.Background(), swarm.ZeroAddress, 0, 0)
		if err != nil {
			t.Fatal(err)
		}

		// Want only two of the offered chunks.
		want := pickRefs(t, refs, addrs[1], addrs[3])

		// INTENDED API: Fetch(ctx, peer, bin, start, want []pullsync.ChunkRef) (count int, err error)
		count, err := client.Fetch(context.Background(), swarm.ZeroAddress, 0, 0, want)
		if err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("fetch count: got %d want 2", count)
		}
		haveChunks(t, clientDB, someChunks(1, 3)...)
		// must NOT have fetched the unrequested ones
		for _, i := range []int{0, 2, 4} {
			stampHash, _ := chunks[i].Stamp().Hash()
			have, _ := clientDB.ReserveHas(chunks[i].Address(), chunks[i].Stamp().BatchID(), stampHash)
			if have {
				t.Fatalf("Fetch stored unrequested chunk %s", chunks[i].Address())
			}
		}
	})
}

// §7.1: Fetching an empty want set is a no-op (no deliveries, no error). The scheduler
// will call Fetch only for peers with a non-empty assigned subset, but the protocol
// must tolerate the empty case cleanly.
func TestFetch_EmptyWantIsNoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			server, _        = newPullSync(t, nil, 5, mock.WithSubscribeResp(results, nil), mock.WithChunks(chunks...))
			recorder         = streamtest.New(streamtest.WithProtocols(server.Protocol()))
			client, clientDB = newPullSync(t, recorder, 0)
		)
		count, err := client.Fetch(context.Background(), swarm.ZeroAddress, 0, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 || clientDB.PutCalls() != 0 {
			t.Fatalf("empty Fetch must be a no-op: count=%d puts=%d", count, clientDB.PutCalls())
		}
	})
}

// pickRefs selects the offered refs whose address is in addrs. Helper for the scaffold;
// fold into the real tests as you see fit.
func pickRefs(t *testing.T, refs []pullsync.ChunkRef, addrs ...swarm.Address) []pullsync.ChunkRef {
	t.Helper()
	var out []pullsync.ChunkRef
	for _, r := range refs {
		for _, a := range addrs {
			if r.Address.Equal(a) {
				out = append(out, r)
			}
		}
	}
	if len(out) != len(addrs) {
		t.Fatalf("pickRefs: matched %d of %d requested", len(out), len(addrs))
	}
	return out
}
