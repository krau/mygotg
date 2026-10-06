package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"gorm.io/gorm"
)

type dialogsInvoker func(context.Context, *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error)

func (f dialogsInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	request, ok := input.(*tg.MessagesGetDialogsRequest)
	if !ok {
		return fmt.Errorf("unexpected request %T", input)
	}
	response, err := f(ctx, request)
	if err != nil {
		return err
	}
	buffer := &bin.Buffer{}
	if err := response.Encode(buffer); err != nil {
		return err
	}
	return output.Decode(buffer)
}

func preloadStorage(t *testing.T, inMemory bool) *PeerStorage {
	t.Helper()
	if inMemory {
		return NewPeerStorage(nil, true)
	}
	p := NewPeerStorage(sqlite.Open(":memory:"), false)
	db, err := p.SqlSession.DB()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return p
}

func preloadClient(response tg.MessagesDialogsClass) *tg.Client {
	return tg.NewClient(dialogsInvoker(func(context.Context, *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
		return response, nil
	}))
}

func TestAddPeersFromDialogsPreservesCompletePeers(t *testing.T) {
	for _, inMemory := range []bool{true, false} {
		t.Run(fmt.Sprintf("inMemory=%t", inMemory), func(t *testing.T) {
			p := preloadStorage(t, inMemory)
			complete := &tg.MessagesDialogs{
				Dialogs: []tg.DialogClass{&tg.Dialog{Peer: &tg.PeerUser{UserID: 1}}},
				Users:   []tg.UserClass{&tg.User{ID: 1, AccessHash: 101, Username: "complete_user"}},
				Chats:   []tg.ChatClass{&tg.Channel{ID: 2, AccessHash: 202, Username: "complete_channel", Photo: &tg.ChatPhotoEmpty{}}},
			}
			if err := AddPeersFromDialogs(context.Background(), preloadClient(complete), p); err != nil {
				t.Fatal(err)
			}
			minimal := &tg.MessagesDialogs{
				Dialogs: complete.Dialogs,
				Users:   []tg.UserClass{&tg.User{ID: 1, Min: true, AccessHash: 999, Username: "min_user"}},
				Chats:   []tg.ChatClass{&tg.Channel{ID: 2, Min: true, AccessHash: 999, Username: "min_channel", Photo: &tg.ChatPhotoEmpty{}}},
			}
			if err := AddPeersFromDialogs(context.Background(), preloadClient(minimal), p); err != nil {
				t.Fatal(err)
			}
			for _, want := range []Peer{
				{ID: 1, AccessHash: 101, Type: TypeUser.GetInt(), Username: "complete_user"},
				{ID: 2, AccessHash: 202, Type: TypeChannel.GetInt(), Username: "complete_channel"},
			} {
				if got := p.GetPeerByIdAndType(want.ID, EntityType(want.Type)); *got != want {
					t.Fatalf("ID lookup = %+v, want %+v", got, want)
				}
				if got := p.GetPeerByUsername(want.Username); *got != want {
					t.Fatalf("username lookup = %+v, want %+v", got, want)
				}
			}
		})
	}
}

func TestAddPeersFromDialogsRPCError(t *testing.T) {
	failure := errors.New("RPC failed")
	for _, want := range []error{failure, context.Canceled} {
		t.Run(want.Error(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			raw := tg.NewClient(dialogsInvoker(func(ctx context.Context, _ *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
				if want == context.Canceled {
					cancel()
					return nil, ctx.Err()
				}
				return nil, want
			}))
			if err := AddPeersFromDialogs(ctx, raw, preloadStorage(t, true)); !errors.Is(err, want) {
				t.Fatalf("error = %v, want %v", err, want)
			}
		})
	}
}

func TestAddPeersFromDialogsStorageError(t *testing.T) {
	for _, want := range []error{errors.New("save failed"), context.Canceled} {
		t.Run(want.Error(), func(t *testing.T) {
			p := preloadStorage(t, false)
			if err := p.SqlSession.Callback().Update().Before("gorm:update").Register("fail_preload", func(db *gorm.DB) {
				db.AddError(want)
			}); err != nil {
				t.Fatal(err)
			}
			response := &tg.MessagesDialogs{
				Dialogs: []tg.DialogClass{&tg.Dialog{Peer: &tg.PeerUser{UserID: 1}}},
				Users:   []tg.UserClass{&tg.User{ID: 1, AccessHash: 101, Username: "user"}},
			}
			if err := AddPeersFromDialogs(context.Background(), preloadClient(response), p); !errors.Is(err, want) {
				t.Fatalf("error = %v, want %v", err, want)
			}
			if got := p.GetPeerById(1); got.ID != 0 {
				t.Fatalf("failed save published cached peer: %+v", got)
			}
			if got := p.GetPeerByUsername("user"); got.ID != 0 {
				t.Fatalf("failed save published stored peer: %+v", got)
			}
		})
	}
}

func TestAddPeersFromDialogsWaitsForPersistence(t *testing.T) {
	p := preloadStorage(t, false)
	entered := make(chan struct{})
	release := make(chan struct{}, 1)
	defer close(release)
	if err := p.SqlSession.Callback().Update().Before("gorm:update").Register("gate_preload", func(*gorm.DB) {
		close(entered)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	response := &tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{&tg.Dialog{Peer: &tg.PeerUser{UserID: 1}}},
		Users:   []tg.UserClass{&tg.User{ID: 1, AccessHash: 101, Username: "saved_user"}},
	}
	done := make(chan error, 1)
	go func() { done <- AddPeersFromDialogs(context.Background(), preloadClient(response), p) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("preload returned before save began: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("save did not begin")
	}
	select {
	case err := <-done:
		t.Fatalf("preload returned while save was blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preload did not finish")
	}
	want := Peer{ID: 1, AccessHash: 101, Type: TypeUser.GetInt(), Username: "saved_user"}
	if got := p.GetPeerById(1); *got != want {
		t.Fatalf("ID lookup = %+v, want %+v", got, want)
	}
	if got := p.GetPeerByUsername(want.Username); *got != want {
		t.Fatalf("username lookup = %+v, want %+v", got, want)
	}
}

func TestAddPeersFromDialogsPaginatesAndSavesEachPageOnce(t *testing.T) {
	p := preloadStorage(t, false)
	saves := make(map[int64]int)
	if err := p.SqlSession.Callback().Update().Before("gorm:update").Register("count_preload", func(db *gorm.DB) {
		if peer, ok := db.Statement.Dest.(*Peer); ok {
			saves[peer.ID]++
		}
	}); err != nil {
		t.Fatal(err)
	}
	const total = 205
	requests := 0
	raw := tg.NewClient(dialogsInvoker(func(_ context.Context, request *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
		requests++
		start := int64(1)
		if offset, ok := request.OffsetPeer.(*tg.InputPeerUser); ok {
			start = offset.UserID + 1
		}
		end := start + int64(request.Limit)
		if end > total+1 {
			end = total + 1
		}
		page := &tg.MessagesDialogsSlice{Count: total}
		for id := start; id < end; id++ {
			page.Dialogs = append(page.Dialogs, &tg.Dialog{Peer: &tg.PeerUser{UserID: id}})
			page.Users = append(page.Users, &tg.User{ID: id, AccessHash: id * 10, Username: fmt.Sprintf("user_%d", id)})
			page.Messages = append(page.Messages, &tg.Message{ID: int(id), Date: int(id), PeerID: &tg.PeerUser{UserID: id}})
		}
		return page, nil
	}))
	if err := AddPeersFromDialogs(context.Background(), raw, p); err != nil {
		t.Fatal(err)
	}
	if requests > 4 {
		t.Fatalf("pagination made %d requests for %d dialogs", requests, total)
	}
	for id := int64(1); id <= total; id++ {
		if saves[id] != 1 {
			t.Fatalf("peer %d saved %d times, want once", id, saves[id])
		}
		if peer := p.GetPeerByUsername(fmt.Sprintf("user_%d", id)); peer.ID != id || peer.AccessHash != id*10 {
			t.Fatalf("peer %d not fully persisted: %+v", id, peer)
		}
	}
}

func TestAddPeersFromDialogsStopsAtFinalPage(t *testing.T) {
	for _, withDialog := range []bool{false, true} {
		t.Run(fmt.Sprintf("withDialog=%t", withDialog), func(t *testing.T) {
			p := preloadStorage(t, false)
			saves := 0
			if err := p.SqlSession.Callback().Update().Before("gorm:update").Register("count_final_page", func(*gorm.DB) {
				saves++
			}); err != nil {
				t.Fatal(err)
			}
			page := &tg.MessagesDialogs{Users: []tg.UserClass{&tg.User{ID: 1, AccessHash: 101, Username: "user"}}}
			if withDialog {
				page.Dialogs = []tg.DialogClass{&tg.Dialog{Peer: &tg.PeerUser{UserID: 1}}}
			}
			requests := 0
			raw := tg.NewClient(dialogsInvoker(func(context.Context, *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
				requests++
				if requests > 1 {
					return nil, errors.New("requested after final page")
				}
				return page, nil
			}))
			if err := AddPeersFromDialogs(context.Background(), raw, p); err != nil {
				t.Fatal(err)
			}
			if requests != 1 || saves != 1 || p.GetPeerByUsername("user").ID != 1 {
				t.Fatalf("final page: requests=%d saves=%d peer=%+v", requests, saves, p.GetPeerByUsername("user"))
			}
		})
	}
}

func TestAddPeersFromDialogsCanceledBeforeRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	raw := tg.NewClient(dialogsInvoker(func(context.Context, *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
		t.Fatal("made an RPC with canceled context")
		return nil, nil
	}))
	if err := AddPeersFromDialogs(ctx, raw, preloadStorage(t, true)); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
}

func TestAddPeersFromDialogsCanceledBeforeSave(t *testing.T) {
	p := preloadStorage(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw := tg.NewClient(dialogsInvoker(func(context.Context, *tg.MessagesGetDialogsRequest) (tg.MessagesDialogsClass, error) {
		cancel()
		return &tg.MessagesDialogs{
			Dialogs: []tg.DialogClass{&tg.Dialog{Peer: &tg.PeerUser{UserID: 1}}},
			Users:   []tg.UserClass{&tg.User{ID: 1, AccessHash: 101, Username: "user"}},
		}, nil
	}))
	if err := AddPeersFromDialogs(ctx, raw, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
	if peer := p.GetPeerById(1); peer.ID != 0 {
		t.Fatalf("canceled save published peer: %+v", peer)
	}
}
