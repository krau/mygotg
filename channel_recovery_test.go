package mygotg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/krau/mygotg/storage"
	"go.uber.org/zap"
)

type channelAPIProbe struct {
	updates.API
	calls  []*tg.UpdatesGetChannelDifferenceRequest
	invoke func(context.Context, *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error)
}

func (a *channelAPIProbe) UpdatesGetChannelDifference(ctx context.Context, request *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
	a.calls = append(a.calls, request)
	return a.invoke(ctx, request)
}

func TestChannelRecoveryRejectsCredentialsWithoutLosingNewerHash(t *testing.T) {
	for _, newer := range []bool{false, true} {
		t.Run(fmt.Sprintf("newer=%t", newer), func(t *testing.T) {
			ctx := context.Background()
			peers := storage.NewPeerStorage(nil, true)
			hasher := storage.NewAccessHasher(peers)
			peers.AddPeer(70, 170, storage.TypeChannel, "synthetic")
			backend := &channelAPIProbe{invoke: func(context.Context, *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
				if newer {
					if err := hasher.SetChannelAccessHash(ctx, 1, 70, 270); err != nil {
						t.Fatal(err)
					}
				}
				return nil, tgerr.New(400, "CHANNEL_INVALID")
			}}
			api := channelRecoveryAPI{API: backend, hasher: hasher, selfID: 1}
			request := &tg.UpdatesGetChannelDifferenceRequest{Channel: &tg.InputChannel{ChannelID: 70, AccessHash: 170}, Pts: 20, Limit: 100, Filter: &tg.ChannelMessagesFilterEmpty{}}
			_, err := api.UpdatesGetChannelDifference(ctx, request)
			if !tgerr.Is(err, "CHANNEL_PRIVATE") || tgerr.Is(err, "CHANNEL_INVALID") {
				t.Fatalf("terminal channel error = %v", err)
			}
			hash, found, err := hasher.GetChannelAccessHash(ctx, 1, 70)
			if err != nil || found {
				t.Fatalf("rejected credentials remain known: hash=%d found=%t err=%v", hash, found, err)
			}
			if got := peers.GetPeerByIdAndType(70, storage.TypeChannel).Username; got != "synthetic" {
				t.Fatalf("username lost: %q", got)
			}
			wantCalls := 1
			if newer {
				wantCalls = 2
			}
			if len(backend.calls) != wantCalls || request.Channel.(*tg.InputChannel).AccessHash != 170 {
				t.Fatalf("calls=%d or caller request mutated", len(backend.calls))
			}
			if err := hasher.SetChannelAccessHash(ctx, 1, 70, 370); err != nil {
				t.Fatal(err)
			}
			if err := hasher.InvalidateChannelAccessHash(ctx, 70, 270); err != nil {
				t.Fatal(err)
			}
			if hash, found, err := hasher.GetChannelAccessHash(ctx, 1, 70); err != nil || !found || hash != 370 {
				t.Fatalf("late rejection cleared newer metadata: %d %t %v", hash, found, err)
			}
		})
	}
}

func TestChannelRecoveryUsesFreshHashWithoutMutatingRequest(t *testing.T) {
	ctx := context.Background()
	peers := storage.NewPeerStorage(nil, true)
	hasher := storage.NewAccessHasher(peers)
	if err := hasher.SetChannelAccessHash(ctx, 1, 70, 270); err != nil {
		t.Fatal(err)
	}
	backend := &channelAPIProbe{invoke: func(_ context.Context, r *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
		if r.Channel.(*tg.InputChannel).AccessHash == 170 {
			return nil, tgerr.New(400, "CHANNEL_INVALID")
		}
		return &tg.UpdatesChannelDifferenceEmpty{Final: true, Pts: 20}, nil
	}}
	api := channelRecoveryAPI{API: backend, hasher: hasher, selfID: 1}
	request := &tg.UpdatesGetChannelDifferenceRequest{Channel: &tg.InputChannel{ChannelID: 70, AccessHash: 170}, Pts: 20, Limit: 100, Filter: &tg.ChannelMessagesFilterEmpty{}}
	diff, err := api.UpdatesGetChannelDifference(ctx, request)
	if err != nil || diff.(*tg.UpdatesChannelDifferenceEmpty).Pts != 20 || len(backend.calls) != 1 {
		t.Fatalf("fresh metadata did not recover: diff=%v err=%v calls=%d", diff, err, len(backend.calls))
	}
	if request.Channel.(*tg.InputChannel).AccessHash != 170 {
		t.Fatal("caller request mutated")
	}
	if _, err := api.UpdatesGetChannelDifference(ctx, request); err != nil || len(backend.calls) != 2 {
		t.Fatalf("later polling reused stale credentials: %v calls=%d", err, len(backend.calls))
	}
	for _, call := range backend.calls {
		if call.Channel.(*tg.InputChannel).AccessHash != 270 {
			t.Fatal("request used obsolete credentials")
		}
	}
	if hash, found, err := hasher.GetChannelAccessHash(ctx, 1, 70); err != nil || !found || hash != 270 {
		t.Fatalf("fresh credentials lost: %d %t %v", hash, found, err)
	}
}

func TestChannelRecoveryPreservesOtherErrors(t *testing.T) {
	for _, failure := range []error{tgerr.New(420, "FLOOD_WAIT_3"), tgerr.New(400, "CHANNEL_PRIVATE"), context.Canceled, errors.New("network failure")} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx := context.Background()
			hasher := storage.NewAccessHasher(storage.NewPeerStorage(nil, true))
			if err := hasher.SetChannelAccessHash(ctx, 1, 70, 170); err != nil {
				t.Fatal(err)
			}
			backend := &channelAPIProbe{invoke: func(context.Context, *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
				return nil, failure
			}}
			api := channelRecoveryAPI{API: backend, hasher: hasher, selfID: 1}
			_, err := api.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{Channel: &tg.InputChannel{ChannelID: 70, AccessHash: 170}})
			if err != failure || len(backend.calls) != 1 {
				t.Fatalf("error=%v calls=%d", err, len(backend.calls))
			}
			if hash, found, err := hasher.GetChannelAccessHash(ctx, 1, 70); err != nil || !found || hash != 170 {
				t.Fatalf("other error discarded credentials: %d %t %v", hash, found, err)
			}
		})
	}
}

func TestInvalidChannelStopsRecoveryAndResumesWithFreshPeer(t *testing.T) {
	var mu sync.Mutex
	var hashes []int64
	channelSaved := make(chan int, 8)
	channelCalls := make(chan int64, 8)
	state := &channelCursorStorage{state: updates.State{Pts: 10, Date: 1}, pts: 20, saved: channelSaved}
	f := newRecoveryFixture(t, func(opts *ClientOpts) {
		opts.UpdateStateStorage = state
		opts.Logger = zap.NewNop()
		original := opts.Middlewares[0]
		opts.Middlewares[0] = telegram.MiddlewareFunc(func(next tg.Invoker) telegram.InvokeFunc {
			invoke := original.Handle(next)
			return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
				request, ok := input.(*tg.UpdatesGetChannelDifferenceRequest)
				if !ok {
					return invoke(ctx, input, output)
				}
				hash := request.Channel.(*tg.InputChannel).AccessHash
				mu.Lock()
				hashes = append(hashes, hash)
				mu.Unlock()
				channelCalls <- hash
				if hash == 170 {
					return tgerr.New(400, "CHANNEL_INVALID")
				}
				output.(*tg.UpdatesChannelDifferenceBox).ChannelDifference = &tg.UpdatesChannelDifferenceEmpty{Final: true, Pts: request.Pts}
				return nil
			}
		})
	})
	hasher := storage.NewAccessHasher(f.client.PeerStorage)
	if err := hasher.SetChannelAccessHash(context.Background(), 1, 70, 170); err != nil {
		t.Fatal(err)
	}
	f.register()
	if err := f.client.StartUpdateRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := recoveryReceive(t, channelCalls); got != 170 {
		t.Fatalf("initial hash = %d", got)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, found, err := hasher.GetChannelAccessHash(deadline, 1, 70)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("rejected hash was not invalidated")
		case <-time.After(time.Millisecond):
		}
	}
	if pts, found, err := state.GetChannelPts(deadline, 1, 70); err != nil || !found || pts != 20 {
		t.Fatalf("rejection lost channel cursor: %d %t %v", pts, found, err)
	}
	if err := f.send(deadline, 777); err != nil {
		t.Fatal(err)
	}
	if got := recoveryReceive(t, f.delivered); got != 777 {
		t.Fatalf("other updates stalled: %d", got)
	}
	// A fresh client run must skip the rejected hash instead of recreating the invalid worker.
	f.client.Stop()
	recoveryReceive(t, f.exited)
	if err := f.client.Start(f.opts); err != nil {
		t.Fatal(err)
	}
	if err := f.client.StartUpdateRecovery(deadline); err != nil {
		t.Fatal(err)
	}
	if err := f.send(deadline, 778); err != nil {
		t.Fatal(err)
	}
	if got := recoveryReceive(t, f.delivered); got != 778 {
		t.Fatalf("restarted common updates stalled: %d", got)
	}
	mu.Lock()
	before := append([]int64(nil), hashes...)
	mu.Unlock()
	if len(before) != 1 {
		t.Fatalf("invalid channel was re-subscribed: %v", before)
	}
	freshChannel := &tg.Channel{ID: 70, Title: "synthetic"}
	freshChannel.SetAccessHash(270)
	update := &tg.Updates{
		Date:  1,
		Chats: []tg.ChatClass{freshChannel},
		Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{
			Message: &tg.Message{ID: 1, PeerID: &tg.PeerChannel{ChannelID: 70}, Post: true, Message: "fresh channel update", Date: 1},
			Pts:     21, PtsCount: 1,
		}},
	}
	if err := f.client.updateHandler().Handle(deadline, update); err != nil {
		t.Fatal(err)
	}
	if got := recoveryReceive(t, channelCalls); got != 270 {
		t.Fatalf("resubscribe used rejected hash: %d", got)
	}
	for {
		if pts := recoveryReceive(t, channelSaved); pts == 21 {
			break
		}
	}
	if pts, found, err := state.GetChannelPts(deadline, 1, 70); err != nil || !found || pts != 21 {
		t.Fatalf("fresh update was not persisted: %d %t %v", pts, found, err)
	}
}

type channelCursorStorage struct {
	updates.StateStorage
	mu    sync.Mutex
	state updates.State
	pts   int
	saved chan int
}

func (s *channelCursorStorage) GetState(context.Context, int64) (updates.State, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, true, nil
}
func (s *channelCursorStorage) SetState(_ context.Context, _ int64, state updates.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}
func (s *channelCursorStorage) SetDateSeq(_ context.Context, _ int64, date, seq int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Date, s.state.Seq = date, seq
	return nil
}
func (s *channelCursorStorage) ForEachChannels(ctx context.Context, _ int64, visit func(context.Context, int64, int) error) error {
	s.mu.Lock()
	pts := s.pts
	s.mu.Unlock()
	return visit(ctx, 70, pts)
}
func (s *channelCursorStorage) GetChannelPts(context.Context, int64, int64) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pts, true, nil
}
func (s *channelCursorStorage) SetChannelPts(_ context.Context, _ int64, _ int64, pts int) error {
	s.mu.Lock()
	s.pts = pts
	s.mu.Unlock()
	s.saved <- pts
	return nil
}
