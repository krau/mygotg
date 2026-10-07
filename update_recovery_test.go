package mygotg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	intErrors "github.com/krau/mygotg/errors"
	"github.com/krau/mygotg/ext"
	"github.com/krau/mygotg/session"
)

type recoveryHandler func(*ext.Context, *ext.Update) error

func (h recoveryHandler) CheckUpdate(ctx *ext.Context, update *ext.Update) error {
	return h(ctx, update)
}

type recoveryFixture struct {
	client      *Client
	opts        *ClientOpts
	delivered   chan int64
	differences chan context.Context
	exited      chan error
	stateCalls  atomic.Int32
}

func newRecoveryFixture(t *testing.T, configure func(*ClientOpts)) *recoveryFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f := &recoveryFixture{
		delivered: make(chan int64, 256), differences: make(chan context.Context, 16), exited: make(chan error, 8),
	}
	f.opts = &ClientOpts{
		Context: ctx, InMemory: true, Session: session.SimpleSession(), DisableCopyright: true,
		DeferUpdateRecovery: true,
		RunMiddleware: func(_ func(context.Context, func(context.Context) error) error, ctx context.Context, run func(context.Context) error) error {
			err := run(ctx)
			f.exited <- err
			return err
		},
		Middlewares: []telegram.Middleware{telegram.MiddlewareFunc(func(tg.Invoker) telegram.InvokeFunc {
			return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				switch input.(type) {
				case *tg.UsersGetUsersRequest:
					output.(*tg.UserClassVector).Elems = []tg.UserClass{&tg.User{ID: 1, Self: true, AccessHash: 1001}}
					return nil
				case *tg.UpdatesGetStateRequest:
					call := f.stateCalls.Add(1)
					*output.(*tg.UpdatesState) = tg.UpdatesState{Pts: int(call) * 10, Date: 1}
					return nil
				case *tg.UpdatesGetDifferenceRequest:
					f.differences <- ctx
					output.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceEmpty{Date: 1}
					return nil
				default:
					return fmt.Errorf("unexpected synthetic RPC %T", input)
				}
			}
		})},
	}
	if configure != nil {
		configure(f.opts)
	}
	client, err := NewClient(1, "test", ClientTypePhone("test"), f.opts)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	f.client = client
	t.Cleanup(func() {
		client.Stop()
		if client.updateRecovery != nil {
			client.updateRecovery.stop()
		}
		cancel()
	})
	return f
}

func (f *recoveryFixture) register() {
	f.client.Dispatcher.AddHandler(recoveryHandler(func(_ *ext.Context, update *ext.Update) error {
		if status, ok := update.UpdateClass.(*tg.UpdateUserStatus); ok {
			f.delivered <- status.UserID
		}
		return nil
	}))
}

func (f *recoveryFixture) send(ctx context.Context, id int64) error {
	return f.client.updateHandler().Handle(ctx, &tg.UpdateShort{
		Update: &tg.UpdateUserStatus{UserID: id, Status: &tg.UserStatusEmpty{}}, Date: 1,
	})
}

func recoveryReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("recovery event timed out")
		var zero T
		return zero
	}
}

func TestDeferredRecoveryBuffersBeforeHandlerRegistration(t *testing.T) {
	f := newRecoveryFixture(t, nil)
	for id := range 150 {
		if err := f.send(context.Background(), int64(id+2)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-f.differences:
		t.Fatal("recovery ran before handler registration")
	default:
	}
	f.register()
	if err := f.client.StartUpdateRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	for id := range 150 {
		if got := recoveryReceive(t, f.delivered); got != int64(id+2) {
			t.Fatalf("delivered ID = %d, want %d", got, id+2)
		}
	}
	if err := f.send(context.Background(), 999); err != nil {
		t.Fatal(err)
	}
	if got := recoveryReceive(t, f.delivered); got != 999 {
		t.Fatalf("live update ID = %d, want 999", got)
	}
}

func TestRecoveryStartContextDoesNotOwnSuccessfulRun(t *testing.T) {
	f := newRecoveryFixture(t, nil)
	f.register()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.client.StartUpdateRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	managerCtx := recoveryReceive(t, f.differences)
	cancel()
	for id := range 20 {
		if err := f.send(context.Background(), int64(id+2)); err != nil {
			t.Fatal(err)
		}
		if got := recoveryReceive(t, f.delivered); got != int64(id+2) {
			t.Fatalf("update ID after start context cancellation = %d", got)
		}
	}
	if managerCtx.Err() != nil {
		t.Fatalf("start context canceled manager: %v", managerCtx.Err())
	}
	f.client.Stop()
	recoveryReceive(t, f.exited)
	if !errors.Is(managerCtx.Err(), context.Canceled) {
		t.Fatalf("manager survived client shutdown: %v", managerCtx.Err())
	}
	if err := f.send(context.Background(), 999); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-stop delivery error = %v", err)
	}
	if err := f.client.StartUpdateRecovery(context.Background()); !errors.Is(err, intErrors.ErrClientNotReady) {
		t.Fatalf("stopped recovery start = %v", err)
	}
}

func TestCanceledRecoveryStartCanRetry(t *testing.T) {
	f := newRecoveryFixture(t, nil)
	f.register()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.client.StartUpdateRecovery(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled start = %v", err)
	}
	if err := f.send(context.Background(), 777); err != nil {
		t.Fatal(err)
	}
	if err := f.client.StartUpdateRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := recoveryReceive(t, f.delivered); got != 777 {
		t.Fatalf("retry lost queued update: got %d", got)
	}
}

type recoveryStorage struct {
	updates.StateStorage
	mu      sync.Mutex
	fail    error
	entered chan struct{}
	block   bool
	states  []updates.State
}

func (s *recoveryStorage) GetState(ctx context.Context, _ int64) (updates.State, bool, error) {
	s.mu.Lock()
	failure, block, entered := s.fail, s.block, s.entered
	s.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if block {
		<-ctx.Done()
		return updates.State{}, false, ctx.Err()
	}
	if failure != nil {
		return updates.State{}, false, failure
	}
	return updates.State{}, false, nil
}

func (s *recoveryStorage) SetState(_ context.Context, _ int64, state updates.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states = append(s.states, state)
	return nil
}

func (*recoveryStorage) ForEachChannels(context.Context, int64, func(context.Context, int64, int) error) error {
	return nil
}

func (*recoveryStorage) SetDateSeq(context.Context, int64, int, int) error { return nil }

func TestRecoverySetupFailureAndCancellationCanRetry(t *testing.T) {
	for _, block := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelDuringLoad=%t", block), func(t *testing.T) {
			failure := errors.New("synthetic state read failure")
			storage := &recoveryStorage{fail: failure, block: block, entered: make(chan struct{}, 4)}
			f := newRecoveryFixture(t, func(opts *ClientOpts) { opts.UpdateStateStorage = storage })
			f.register()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- f.client.StartUpdateRecovery(ctx) }()
			recoveryReceive(t, storage.entered)
			if block {
				if err := f.client.StartUpdateRecovery(context.Background()); !errors.Is(err, intErrors.ErrUpdateRecoveryStarted) {
					t.Fatalf("concurrent start = %v", err)
				}
				cancel()
			}
			err := recoveryReceive(t, result)
			if block && !errors.Is(err, context.Canceled) || !block && !errors.Is(err, failure) {
				t.Fatalf("setup result = %v", err)
			}
			storage.mu.Lock()
			storage.fail, storage.block = nil, false
			storage.mu.Unlock()
			if err := f.client.StartUpdateRecovery(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := f.send(context.Background(), 777); err != nil {
				t.Fatal(err)
			}
			if got := recoveryReceive(t, f.delivered); got != 777 {
				t.Fatalf("retry update ID = %d", got)
			}
		})
	}
}

func TestDeferredFirstStartUsesLoginTimeState(t *testing.T) {
	storage := &recoveryStorage{}
	f := newRecoveryFixture(t, func(opts *ClientOpts) { opts.UpdateStateStorage = storage })
	f.register()
	if err := f.send(context.Background(), 777); err != nil {
		t.Fatal(err)
	}
	if err := f.client.StartUpdateRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := recoveryReceive(t, f.delivered); got != 777 {
		t.Fatalf("queued update ID = %d", got)
	}
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if len(storage.states) != 1 || storage.states[0].Pts != 10 {
		t.Fatalf("initial persisted state = %v, want login-time pts=10", storage.states)
	}
}

func TestRecoveryRestartReadinessAndDefaultModes(t *testing.T) {
	f := newRecoveryFixture(t, nil)
	f.register()
	if err := f.client.StartUpdateRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	firstCtx := recoveryReceive(t, f.differences)
	f.client.Stop()
	recoveryReceive(t, f.exited)
	if err := f.client.Start(f.opts); err != nil {
		t.Fatal(err)
	}
	if firstCtx.Err() == nil {
		t.Fatal("old recovery survived restart")
	}
	if err := f.send(context.Background(), 777); err != nil {
		t.Fatal(err)
	}
	if err := f.client.StartUpdateRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := recoveryReceive(t, f.delivered); got != 777 {
		t.Fatalf("restart update = %d", got)
	}
	f.client.Stop()
	recoveryReceive(t, f.exited)
	failure := errors.New("synthetic restart auth failure")
	failedOpts := *f.opts
	failedOpts.RunMiddleware = func(func(context.Context, func(context.Context) error) error, context.Context, func(context.Context) error) error {
		return failure
	}
	if err := f.client.Start(&failedOpts); !errors.Is(err, failure) {
		t.Fatalf("failed restart = %v", err)
	}
	if err := f.client.StartUpdateRecovery(context.Background()); !errors.Is(err, intErrors.ErrClientNotReady) {
		t.Fatalf("recovery after failed restart = %v", err)
	}

	for _, mode := range []string{"automatic", "disabled", "noUpdates"} {
		t.Run(mode, func(t *testing.T) {
			f := newRecoveryFixture(t, func(opts *ClientOpts) {
				opts.DeferUpdateRecovery = mode != "automatic"
				opts.DisableUpdateRecovery = mode == "disabled"
				opts.NoUpdates = mode == "noUpdates"
			})
			if mode == "automatic" {
				recoveryReceive(t, f.differences)
				if err := f.client.StartUpdateRecovery(context.Background()); !errors.Is(err, intErrors.ErrUpdateRecoveryStarted) {
					t.Fatalf("automatic repeated start = %v", err)
				}
			} else if err := f.client.StartUpdateRecovery(context.Background()); !errors.Is(err, intErrors.ErrUpdateRecoveryOff) {
				t.Fatalf("disabled recovery start = %v", err)
			}
		})
	}
}
