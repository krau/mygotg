package storage

import (
	"context"
	"fmt"
	"testing"
)

func TestAccessHasherRoundTrip(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%t", inMemory), func(t *testing.T) {
			peers := preloadStorage(t, inMemory)
			hasher := NewAccessHasher(peers)
			ctx := context.Background()

			if _, found, err := hasher.GetChannelAccessHash(ctx, 1, 777); err != nil || found {
				t.Fatalf("unknown channel: found=%t err=%v, want found=false err=nil", found, err)
			}
			if err := hasher.SetChannelAccessHash(ctx, 1, 777, 4242); err != nil {
				t.Fatal(err)
			}
			hash, found, err := hasher.GetChannelAccessHash(ctx, 1, 777)
			if err != nil || !found || hash != 4242 {
				t.Fatalf("channel hash = (%d, %t, %v), want (4242, true, nil)", hash, found, err)
			}

			if err := hasher.SetUserAccessHash(ctx, 1, 888, 2424); err != nil {
				t.Fatal(err)
			}
			hash, found, err = hasher.GetUserAccessHash(ctx, 1, 888)
			if err != nil || !found || hash != 2424 {
				t.Fatalf("user hash = (%d, %t, %v), want (2424, true, nil)", hash, found, err)
			}
		})
	}
}

func TestAccessHasherPreservesUsername(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%t", inMemory), func(t *testing.T) {
			peers := preloadStorage(t, inMemory)
			hasher := NewAccessHasher(peers)
			ctx := context.Background()

			peers.AddPeer(777, 1, TypeChannel, "chan_777")
			if err := hasher.SetChannelAccessHash(ctx, 1, 777, 4242); err != nil {
				t.Fatal(err)
			}
			peer := peers.GetPeerByIdAndType(777, TypeChannel)
			if peer.AccessHash != 4242 {
				t.Fatalf("access hash = %d, want 4242", peer.AccessHash)
			}
			if peer.Username != "chan_777" {
				t.Fatalf("username = %q, want %q", peer.Username, "chan_777")
			}
		})
	}
}
