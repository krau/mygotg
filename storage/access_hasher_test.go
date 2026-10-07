package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
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

			if err := hasher.SetUserAccessHash(ctx, 1, 777, 2424); err != nil {
				t.Fatal(err)
			}
			hash, found, err = hasher.GetUserAccessHash(ctx, 1, 777)
			if err != nil || !found || hash != 2424 {
				t.Fatalf("user hash = (%d, %t, %v), want (2424, true, nil)", hash, found, err)
			}
			hash, found, err = hasher.GetChannelAccessHash(ctx, 1, 777)
			if err != nil || !found || hash != 4242 {
				t.Fatalf("channel hash after user write = (%d, %t, %v), want (4242, true, nil)", hash, found, err)
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

func TestAccessHasherZeroHashIsUnknown(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%t", inMemory), func(t *testing.T) {
			peers := preloadStorage(t, inMemory)
			hasher := NewAccessHasher(peers)
			ctx := context.Background()
			for _, peerType := range []EntityType{TypeUser, TypeChannel} {
				peer := &Peer{ID: 777, Type: peerType.GetInt(), Username: "zero_hash"}
				if err := peers.savePreloadedPeer(ctx, peer); err != nil {
					t.Fatal(err)
				}
				if !inMemory {
					peers.peerCache.Delete(PeerKey{ID: peer.ID, Type: peer.Type})
				}
				get := hasher.GetChannelAccessHash
				set := hasher.SetChannelAccessHash
				if peerType == TypeUser {
					get = hasher.GetUserAccessHash
					set = hasher.SetUserAccessHash
				}
				hash, found, err := get(ctx, 1, peer.ID)
				if err != nil || found || hash != 0 {
					t.Fatalf("zero hash for type %d = (%d, %t, %v), want (0, false, nil)", peerType, hash, found, err)
				}
				if got := peers.GetPeerByIdAndType(peer.ID, peerType); *got != *peer {
					t.Fatalf("zero-hash peer = %+v, want %+v", got, peer)
				}
				if err := set(ctx, 1, peer.ID, -4242); err != nil {
					t.Fatal(err)
				}
				if hash, found, err := get(ctx, 1, peer.ID); err != nil || !found || hash != -4242 {
					t.Fatalf("signed hash for type %d = (%d, %t, %v), want (-4242, true, nil)", peerType, hash, found, err)
				}
				if err := set(ctx, 1, peer.ID, 0); err != nil {
					t.Fatal(err)
				}
				if hash, found, err := get(ctx, 1, peer.ID); err != nil || found || hash != 0 {
					t.Fatalf("zero setter hash for type %d = (%d, %t, %v), want (0, false, nil)", peerType, hash, found, err)
				}
				if got := peers.GetPeerByIdAndType(peer.ID, peerType); *got != *peer {
					t.Fatalf("zero setter peer = %+v, want %+v", got, peer)
				}
			}
		})
	}
}

func TestAccessHasherCanceledContext(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%t", inMemory), func(t *testing.T) {
			peers := preloadStorage(t, inMemory)
			hasher := NewAccessHasher(peers)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for _, peerType := range []EntityType{TypeUser, TypeChannel} {
				if err := peers.savePreloadedPeer(context.Background(), &Peer{ID: 777, AccessHash: 4242, Type: peerType.GetInt()}); err != nil {
					t.Fatal(err)
				}
				for _, id := range []int64{777, 888} {
					var hash int64
					var found bool
					var err error
					if peerType == TypeUser {
						hash, found, err = hasher.GetUserAccessHash(ctx, 1, id)
					} else {
						hash, found, err = hasher.GetChannelAccessHash(ctx, 1, id)
					}
					if !errors.Is(err, context.Canceled) || found || hash != 0 {
						t.Fatalf("canceled lookup for type %d ID %d = (%d, %t, %v), want (0, false, context.Canceled)", peerType, id, hash, found, err)
					}
				}
				var err error
				if peerType == TypeUser {
					err = hasher.SetUserAccessHash(ctx, 1, 777, 999)
				} else {
					err = hasher.SetChannelAccessHash(ctx, 1, 777, 999)
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled setter error = %v, want context.Canceled", err)
				}
				if got := peers.GetPeerByIdAndType(777, peerType); got.AccessHash != 4242 {
					t.Fatalf("canceled setter changed peer: %+v", got)
				}
			}
		})
	}
}

func TestAccessHasherLookupErrors(t *testing.T) {
	for _, failure := range []string{"query", "closed_db"} {
		t.Run(failure, func(t *testing.T) {
			peers := preloadStorage(t, false)
			hasher := NewAccessHasher(peers)
			ctx := context.Background()
			for _, get := range []func(context.Context, int64, int64) (int64, bool, error){hasher.GetUserAccessHash, hasher.GetChannelAccessHash} {
				if hash, found, err := get(ctx, 1, 777); hash != 0 || found || err != nil {
					t.Fatalf("missing peer = (%d, %t, %v), want (0, false, nil)", hash, found, err)
				}
			}
			var want error
			if failure == "query" {
				want = errors.New("peer query failed")
				if err := peers.SqlSession.Callback().Query().Before("gorm:query").Register("fail_hash_lookup", func(db *gorm.DB) {
					db.AddError(want)
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				db, err := peers.SqlSession.DB()
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for _, get := range []func(context.Context, int64, int64) (int64, bool, error){hasher.GetUserAccessHash, hasher.GetChannelAccessHash} {
				hash, found, err := get(ctx, 1, 777)
				if hash != 0 || found || err == nil || (want != nil && !errors.Is(err, want)) {
					t.Fatalf("failed lookup = (%d, %t, %v), want DB error without a peer", hash, found, err)
				}
			}
			for _, set := range []func(context.Context, int64, int64, int64) error{hasher.SetUserAccessHash, hasher.SetChannelAccessHash} {
				if err := set(ctx, 1, 777, 4242); err == nil || (want != nil && !errors.Is(err, want)) {
					t.Fatalf("failed username lookup setter error = %v, want DB error", err)
				}
			}
		})
	}
}

func TestAccessHasherSaveFailurePreservesPeer(t *testing.T) {
	peers := preloadStorage(t, false)
	ctx := context.Background()
	want := Peer{ID: 777, AccessHash: 101, Type: TypeChannel.GetInt(), Username: "complete_channel"}
	if err := peers.savePreloadedPeer(ctx, &want); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("hash save failed")
	if err := peers.SqlSession.Callback().Update().Before("gorm:update").Register("fail_hash_save", func(db *gorm.DB) {
		db.AddError(failure)
	}); err != nil {
		t.Fatal(err)
	}
	if err := NewAccessHasher(peers).SetChannelAccessHash(ctx, 1, want.ID, 4242); !errors.Is(err, failure) {
		t.Fatalf("setter error = %v, want %v", err, failure)
	}
	if got := peers.GetPeerByIdAndType(want.ID, TypeChannel); *got != want {
		t.Fatalf("failed setter changed cache: %+v, want %+v", got, want)
	}
	var stored Peer
	if err := peers.SqlSession.Where("id = ? AND type = ?", want.ID, want.Type).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored != want {
		t.Fatalf("failed setter changed DB: %+v, want %+v", stored, want)
	}
}

func persistentHasherStorage(t *testing.T, path string) *PeerStorage {
	t.Helper()
	peers := NewPeerStorage(sqlite.Open(path), false)
	db, err := peers.SqlSession.DB()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return peers
}

func reopenHasherStorage(t *testing.T, peers *PeerStorage, path string) *PeerStorage {
	t.Helper()
	db, err := peers.SqlSession.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return persistentHasherStorage(t, path)
}

func TestAccessHasherPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.sqlite")
	peers := persistentHasherStorage(t, path)
	ctx := context.Background()
	for _, peerType := range []EntityType{TypeUser, TypeChannel} {
		peer := &Peer{ID: 777, AccessHash: 1, Type: peerType.GetInt(), Username: fmt.Sprintf("peer_%d", peerType)}
		if err := peers.savePreloadedPeer(ctx, peer); err != nil {
			t.Fatal(err)
		}
		peers.peerCache.Delete(PeerKey{ID: peer.ID, Type: peer.Type})
	}
	hasher := NewAccessHasher(peers)
	if err := hasher.SetUserAccessHash(ctx, 1, 777, 2424); err != nil {
		t.Fatal(err)
	}
	if err := hasher.SetChannelAccessHash(ctx, 1, 777, 4242); err != nil {
		t.Fatal(err)
	}
	peers = reopenHasherStorage(t, peers, path)
	hasher = NewAccessHasher(peers)
	for _, test := range []struct {
		peerType EntityType
		hash     int64
		get      func(context.Context, int64, int64) (int64, bool, error)
	}{
		{peerType: TypeUser, hash: 2424, get: hasher.GetUserAccessHash},
		{peerType: TypeChannel, hash: 4242, get: hasher.GetChannelAccessHash},
	} {
		if hash, found, err := test.get(ctx, 1, 777); err != nil || !found || hash != test.hash {
			t.Fatalf("reopened hash for type %d = (%d, %t, %v), want (%d, true, nil)", test.peerType, hash, found, err, test.hash)
		}
		want := Peer{ID: 777, AccessHash: test.hash, Type: test.peerType.GetInt(), Username: fmt.Sprintf("peer_%d", test.peerType)}
		if got := peers.GetPeerByIdAndType(want.ID, test.peerType); *got != want {
			t.Fatalf("reopened peer = %+v, want %+v", got, want)
		}
	}
}

func TestAddPeerReturnsBeforePersistence(t *testing.T) {
	peers := preloadStorage(t, false)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	if err := peers.SqlSession.Callback().Update().Before("gorm:update").Register("gate_async_peer", func(*gorm.DB) {
		close(entered)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	returned := make(chan struct{})
	go func() {
		peers.AddPeer(777, 4242, TypeChannel, "async_channel")
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("AddPeer waited for persistence")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("async save did not begin")
	}
	hasher := NewAccessHasher(peers)
	if hash, found, err := hasher.GetChannelAccessHash(context.Background(), 1, 777); err != nil || !found || hash != 4242 {
		t.Fatalf("cached async hash = (%d, %t, %v), want (4242, true, nil)", hash, found, err)
	}
	unblock()
	peers.peerLock.Lock()
	peers.peerLock.Unlock()
	var stored Peer
	if err := peers.SqlSession.Where("id = ? AND type = ?", 777, TypeChannel.GetInt()).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.AccessHash != 4242 || stored.Username != "async_channel" {
		t.Fatalf("async peer not persisted: %+v", stored)
	}
}

func TestPendingAddPeerCannotOverwriteSynchronousSave(t *testing.T) {
	for _, writer := range []string{"hasher", "preload"} {
		t.Run(writer, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "peers.sqlite")
			peers := persistentHasherStorage(t, path)
			old := &Peer{ID: 777, AccessHash: 1, Type: TypeChannel.GetInt(), Username: "queued_channel"}
			key := PeerKey{ID: old.ID, Type: old.Type}
			// Gate the queued worker before it acquires the serializer, not inside a DB callback.
			peers.peerLock.Lock()
			peers.peerCache.Set(key, old)
			peers.pendingPeers = map[PeerKey]*Peer{key: old}
			peers.peerLock.Unlock()
			release := make(chan struct{})
			done := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer func() {
				unblock()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("queued worker did not stop during cleanup")
				}
			}()
			go func() {
				<-release
				peers.addPeerToDb(old)
				close(done)
			}()
			saves := 0
			if err := peers.SqlSession.Callback().Update().Before("gorm:update").Register("count_ordered_save", func(*gorm.DB) {
				saves++
			}); err != nil {
				t.Fatal(err)
			}
			want := Peer{ID: old.ID, AccessHash: 4242, Type: old.Type, Username: old.Username}
			if writer == "hasher" {
				if err := NewAccessHasher(peers).SetChannelAccessHash(context.Background(), 1, old.ID, want.AccessHash); err != nil {
					t.Fatal(err)
				}
			} else {
				want.Username = "preloaded_channel"
				if err := peers.savePreloadedPeer(context.Background(), &want); err != nil {
					t.Fatal(err)
				}
			}
			unblock()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("superseded worker did not finish")
			}
			if saves != 1 {
				t.Fatalf("DB saves = %d, want only the synchronous save", saves)
			}
			if got := peers.GetPeerByIdAndType(old.ID, TypeChannel); *got != want {
				t.Fatalf("cached peer = %+v, want %+v", got, want)
			}
			peers = reopenHasherStorage(t, peers, path)
			if got := peers.GetPeerByIdAndType(old.ID, TypeChannel); *got != want {
				t.Fatalf("reopened peer = %+v, want %+v", got, want)
			}
		})
	}
}

func TestPendingAddPeerSurvivesCacheEviction(t *testing.T) {
	peers := preloadStorage(t, false)
	ctx := context.Background()
	old := &Peer{ID: 777, AccessHash: 1, Type: TypeChannel.GetInt(), Username: "old_channel"}
	if err := peers.savePreloadedPeer(ctx, old); err != nil {
		t.Fatal(err)
	}
	latest := &Peer{ID: old.ID, AccessHash: 4242, Type: old.Type, Username: "latest_channel"}
	key := PeerKey{ID: latest.ID, Type: latest.Type}
	peers.peerLock.Lock()
	peers.pendingPeers = map[PeerKey]*Peer{key: latest}
	peers.peerCache.Set(key, latest)
	peers.peerCache.Delete(key)
	peers.peerLock.Unlock()
	hasher := NewAccessHasher(peers)
	if hash, found, err := hasher.GetChannelAccessHash(ctx, 1, latest.ID); err != nil || !found || hash != latest.AccessHash {
		t.Fatalf("evicted pending hash = (%d, %t, %v), want (%d, true, nil)", hash, found, err, latest.AccessHash)
	}
	if err := hasher.SetChannelAccessHash(ctx, 1, latest.ID, 999); err != nil {
		t.Fatal(err)
	}
	peers.addPeerToDb(latest)
	peers.peerCache.Delete(key)
	want := Peer{ID: latest.ID, AccessHash: 999, Type: latest.Type, Username: latest.Username}
	if got := peers.GetPeerByIdAndType(latest.ID, TypeChannel); *got != want {
		t.Fatalf("stored peer after pending cache eviction = %+v, want %+v", got, want)
	}
}

func TestAccessHasherUsernameUpdateIsAtomic(t *testing.T) {
	for _, writer := range []string{"add_peer", "preload"} {
		t.Run(writer, func(t *testing.T) {
			peers := preloadStorage(t, false)
			ctx := context.Background()
			old := &Peer{ID: 777, AccessHash: 1, Type: TypeChannel.GetInt(), Username: "old_channel"}
			if err := peers.savePreloadedPeer(ctx, old); err != nil {
				t.Fatal(err)
			}
			peers.peerCache.Delete(PeerKey{ID: old.ID, Type: old.Type})
			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			if err := peers.SqlSession.Callback().Query().After("gorm:query").Register("gate_username_lookup", func(*gorm.DB) {
				close(entered)
				<-release
			}); err != nil {
				t.Fatal(err)
			}
			hashDone := make(chan error, 1)
			go func() { hashDone <- NewAccessHasher(peers).SetChannelAccessHash(ctx, 1, old.ID, 4242) }()
			select {
			case <-entered:
			case err := <-hashDone:
				t.Fatalf("setter returned before username lookup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("username lookup did not begin")
			}
			if peers.peerLock.TryLock() {
				peers.peerLock.Unlock()
				t.Fatal("username lookup is not protected by the mutation lock")
			}
			want := Peer{ID: old.ID, AccessHash: 999, Type: old.Type, Username: "latest_channel"}
			writeDone := make(chan error, 1)
			go func() {
				if writer == "add_peer" {
					peers.AddPeer(want.ID, want.AccessHash, TypeChannel, want.Username)
					writeDone <- nil
				} else {
					writeDone <- peers.savePreloadedPeer(ctx, &want)
				}
			}()
			unblock()
			for _, done := range []<-chan error{hashDone, writeDone} {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("serialized mutation did not finish")
				}
			}
			if err := peers.SqlSession.Callback().Query().Remove("gate_username_lookup"); err != nil {
				t.Fatal(err)
			}
			if writer == "add_peer" {
				peers.peerLock.Lock()
				pending := peers.pendingPeers[PeerKey{ID: want.ID, Type: want.Type}]
				peers.peerLock.Unlock()
				if pending != nil {
					peers.addPeerToDb(pending)
				}
			}
			if got := peers.GetPeerByIdAndType(want.ID, TypeChannel); *got != want {
				t.Fatalf("latest username mutation lost: %+v, want %+v", got, want)
			}
			var stored Peer
			if err := peers.SqlSession.Where("id = ? AND type = ?", want.ID, want.Type).First(&stored).Error; err != nil {
				t.Fatal(err)
			}
			if stored != want {
				t.Fatalf("latest username mutation not persisted: %+v, want %+v", stored, want)
			}
		})
	}
}

func TestPeerLookupCannotRepopulateStaleCache(t *testing.T) {
	for _, lookup := range []string{"id", "typed_id"} {
		t.Run(lookup, func(t *testing.T) {
			peers := preloadStorage(t, false)
			ctx := context.Background()
			old := &Peer{ID: 777, AccessHash: 1, Type: TypeChannel.GetInt(), Username: "old_channel"}
			if err := peers.savePreloadedPeer(ctx, old); err != nil {
				t.Fatal(err)
			}
			peers.peerCache.Delete(PeerKey{ID: old.ID, Type: old.Type})
			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			if err := peers.SqlSession.Callback().Query().After("gorm:query").Register("gate_peer_cache_fill", func(*gorm.DB) {
				close(entered)
				<-release
			}); err != nil {
				t.Fatal(err)
			}
			readDone := make(chan *Peer, 1)
			go func() {
				if lookup == "id" {
					readDone <- peers.GetPeerById(old.ID)
				} else {
					readDone <- peers.GetPeerByIdAndType(old.ID, TypeChannel)
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("peer lookup did not begin")
			}
			if peers.peerLock.TryLock() {
				peers.peerLock.Unlock()
				t.Fatal("cache fill is not protected by the mutation lock")
			}
			want := Peer{ID: old.ID, AccessHash: 4242, Type: old.Type, Username: "latest_channel"}
			writeDone := make(chan error, 1)
			go func() { writeDone <- peers.savePreloadedPeer(ctx, &want) }()
			unblock()
			select {
			case got := <-readDone:
				if *got != *old {
					t.Fatalf("initial lookup = %+v, want %+v", got, old)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("peer lookup did not finish")
			}
			select {
			case err := <-writeDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("peer mutation did not finish")
			}
			if err := peers.SqlSession.Callback().Query().Remove("gate_peer_cache_fill"); err != nil {
				t.Fatal(err)
			}
			if got := peers.GetPeerByIdAndType(want.ID, TypeChannel); *got != want {
				t.Fatalf("stale lookup replaced latest cache: %+v, want %+v", got, want)
			}
		})
	}
}

func TestPendingAddPeerCannotOverwriteNewerAddPeer(t *testing.T) {
	peers := preloadStorage(t, false)
	old := &Peer{ID: 777, AccessHash: 1, Type: TypeChannel.GetInt(), Username: "old_channel"}
	key := PeerKey{ID: old.ID, Type: old.Type}
	peers.peerLock.Lock()
	peers.peerCache.Set(key, old)
	peers.pendingPeers = map[PeerKey]*Peer{key: old}
	peers.peerLock.Unlock()
	want := Peer{ID: old.ID, AccessHash: 4242, Type: old.Type, Username: "latest_channel"}
	peers.AddPeer(want.ID, want.AccessHash, TypeChannel, want.Username)
	peers.addPeerToDb(old)
	peers.peerLock.Lock()
	pending := peers.pendingPeers[key]
	peers.peerLock.Unlock()
	if pending != nil {
		peers.addPeerToDb(pending)
	}
	if got := peers.GetPeerByIdAndType(want.ID, TypeChannel); *got != want {
		t.Fatalf("newer AddPeer lost from cache: %+v, want %+v", got, want)
	}
	var stored Peer
	if err := peers.SqlSession.Where("id = ? AND type = ?", want.ID, want.Type).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored != want {
		t.Fatalf("newer AddPeer lost from DB: %+v, want %+v", stored, want)
	}
}

func TestAccessHasherCancellationDuringLookup(t *testing.T) {
	for _, peerType := range []EntityType{TypeUser, TypeChannel} {
		t.Run(fmt.Sprintf("type=%d", peerType), func(t *testing.T) {
			peers := preloadStorage(t, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := peers.SqlSession.Callback().Query().Before("gorm:query").Register("cancel_hash_lookup", func(*gorm.DB) {
				cancel()
			}); err != nil {
				t.Fatal(err)
			}
			hasher := NewAccessHasher(peers)
			get := hasher.GetChannelAccessHash
			if peerType == TypeUser {
				get = hasher.GetUserAccessHash
			}
			if hash, found, err := get(ctx, 1, 777); !errors.Is(err, context.Canceled) || found || hash != 0 {
				t.Fatalf("canceled DB lookup = (%d, %t, %v), want (0, false, context.Canceled)", hash, found, err)
			}
		})
	}
}
