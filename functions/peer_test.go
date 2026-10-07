package functions_test

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/krau/mygotg/functions"
	"github.com/krau/mygotg/storage"
)

func TestSavePeersFromClassArraySkipsMinEntities(t *testing.T) {
	peers := storage.NewPeerStorage(nil, true)
	hasher := storage.NewAccessHasher(peers)
	ctx := context.Background()
	functions.SavePeersFromClassArray(peers,
		[]tg.ChatClass{
			&tg.Channel{ID: 2, AccessHash: 202, Username: "complete_channel"},
			&tg.Chat{ID: 3},
		},
		[]tg.UserClass{&tg.User{ID: 1, AccessHash: 101, Username: "complete_user"}},
	)
	functions.SavePeersFromClassArray(peers,
		[]tg.ChatClass{
			&tg.Channel{ID: 2, Min: true, AccessHash: 999, Username: "min_channel"},
			&tg.Channel{ID: 4, Min: true, AccessHash: 404, Username: "unknown_min_channel"},
			&tg.Channel{ID: 6, Username: "zero_channel"},
		},
		[]tg.UserClass{
			&tg.User{ID: 1, Min: true, AccessHash: 999, Username: "min_user"},
			&tg.User{ID: 5, Min: true, AccessHash: 505, Username: "unknown_min_user"},
			&tg.User{ID: 7, Username: "zero_user"},
			&tg.UserEmpty{ID: 8},
		},
	)
	for _, want := range []storage.Peer{
		{ID: 1, AccessHash: 101, Type: storage.TypeUser.GetInt(), Username: "complete_user"},
		{ID: 2, AccessHash: 202, Type: storage.TypeChannel.GetInt(), Username: "complete_channel"},
		{ID: 3, Type: storage.TypeChat.GetInt()},
		{ID: 6, Type: storage.TypeChannel.GetInt(), Username: "zero_channel"},
		{ID: 7, Type: storage.TypeUser.GetInt(), Username: "zero_user"},
	} {
		if got := peers.GetPeerByIdAndType(want.ID, storage.EntityType(want.Type)); *got != want {
			t.Fatalf("stored peer = %+v, want %+v", got, want)
		}
	}
	for _, test := range []struct {
		id    int64
		hash  int64
		found bool
		get   func(context.Context, int64, int64) (int64, bool, error)
	}{
		{id: 1, hash: 101, found: true, get: hasher.GetUserAccessHash},
		{id: 2, hash: 202, found: true, get: hasher.GetChannelAccessHash},
		{id: 4, get: hasher.GetChannelAccessHash},
		{id: 5, get: hasher.GetUserAccessHash},
		{id: 6, get: hasher.GetChannelAccessHash},
		{id: 7, get: hasher.GetUserAccessHash},
		{id: 8, get: hasher.GetUserAccessHash},
	} {
		if hash, found, err := test.get(ctx, 1, test.id); err != nil || hash != test.hash || found != test.found {
			t.Fatalf("hash for ID %d = (%d, %t, %v), want (%d, %t, nil)", test.id, hash, found, err, test.hash, test.found)
		}
	}
}
