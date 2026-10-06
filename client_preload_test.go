package mygotg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/krau/mygotg/dispatcher"
	"github.com/krau/mygotg/ext"
	"github.com/krau/mygotg/session"
	"github.com/krau/mygotg/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func peerPreloadTestClient(t *testing.T) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Client{
		ctx:         ctx,
		cancel:      cancel,
		running:     true,
		Self:        &tg.User{ID: 1},
		PeerStorage: storage.NewPeerStorage(nil, true),
		Client:      telegram.NewClient(1, "test", telegram.Options{}),
	}
}

func TestPeerPreloadErrorHandler(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprintf("stop=%t", stop), func(t *testing.T) {
			c := peerPreloadTestClient(t)
			called := false
			opts := &ClientOpts{ErrorHandler: func(_ *ext.Context, _ *ext.Update, message string) error {
				called = true
				if !strings.Contains(message, "preload peers from dialogs") || !strings.Contains(message, "RPC failure") {
					t.Fatalf("missing error context: %q", message)
				}
				if stop {
					return fmt.Errorf("requested stop: %w", dispatcher.StopClient)
				}
				return dispatcher.ContinueGroups
			}}
			c.handlePeerPreloadError(opts, errors.New("RPC failure"))
			if !called {
				t.Fatal("error handler not called")
			}
			if got := c.ctx.Err() != nil; got != stop || c.running == stop {
				t.Fatalf("stopped=%t running=%t, want stopped=%t", got, c.running, stop)
			}
		})
	}
}

func TestPeerPreloadErrorLogging(t *testing.T) {
	c := peerPreloadTestClient(t)
	core, observed := observer.New(zap.ErrorLevel)
	c.Logger = zap.New(core)
	c.handlePeerPreloadError(&ClientOpts{}, errors.New("storage failure"))
	entries := observed.All()
	if len(entries) != 1 || !strings.Contains(entries[0].ContextMap()["error"].(string), "storage failure") {
		t.Fatalf("unexpected error log: %+v", entries)
	}
	c.Logger = nil
	var output bytes.Buffer
	writer := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(writer) })
	c.handlePeerPreloadError(&ClientOpts{}, errors.New("fallback failure"))
	if !strings.Contains(output.String(), "preload peers from dialogs: fallback failure") {
		t.Fatalf("missing standard log fallback: %q", output.String())
	}
}

func TestPeerPreloadCanceledShutdownIsQuiet(t *testing.T) {
	c := peerPreloadTestClient(t)
	called := false
	opts := &ClientOpts{ErrorHandler: func(*ext.Context, *ext.Update, string) error {
		called = true
		return dispatcher.ContinueGroups
	}}
	c.handlePeerPreloadError(opts, context.Canceled)
	if !called {
		t.Fatal("cancellation unrelated to shutdown was swallowed")
	}
	called = false
	c.Stop()
	c.handlePeerPreloadError(opts, fmt.Errorf("RPC canceled: %w", context.Canceled))
	if called {
		t.Fatal("shutdown cancellation was reported as an error")
	}
}

func TestStartPeerPreloadFailureStopsSafely(t *testing.T) {
	for _, wait := range []bool{true, false} {
		t.Run(fmt.Sprintf("wait=%t", wait), func(t *testing.T) {
			failure := errors.New("dialogs failed")
			runExited := make(chan error, 1)
			reported := make(chan string, 1)
			started := make(chan struct{})
			opts := &ClientOpts{
				InMemory:               true,
				Session:                session.SimpleSession(),
				NoUpdates:              true,
				DisableCopyright:       true,
				PeersFromDialogs:       true,
				WaitOnPeersFromDialogs: wait,
				RunMiddleware: func(_ func(context.Context, func(context.Context) error) error, ctx context.Context, f func(context.Context) error) error {
					err := f(ctx)
					runExited <- err
					return err
				},
				ErrorHandler: func(_ *ext.Context, _ *ext.Update, message string) error {
					reported <- message
					if !wait {
						<-started
					}
					return dispatcher.StopClient
				},
				Middlewares: []telegram.Middleware{telegram.MiddlewareFunc(func(tg.Invoker) telegram.InvokeFunc {
					return func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
						switch input.(type) {
						case *tg.UsersGetUsersRequest:
							buffer := &bin.Buffer{}
							users := &tg.UserClassVector{Elems: []tg.UserClass{&tg.User{ID: 1, AccessHash: 101}}}
							if err := users.Encode(buffer); err != nil {
								return err
							}
							return output.Decode(buffer)
						case *tg.MessagesGetDialogsRequest:
							return failure
						default:
							return fmt.Errorf("unexpected RPC %T", input)
						}
					}
				})},
			}
			if !wait {
				defer close(started)
			}
			client, err := NewClient(1, "test", ClientTypePhone("test"), opts)
			if client != nil {
				t.Cleanup(client.Stop)
			}
			if wait {
				if !errors.Is(err, failure) || !strings.Contains(err.Error(), "preload peers from dialogs") {
					t.Fatalf("startup error = %v, want wrapped dialogs failure", err)
				}
			} else {
				if err != nil {
					t.Fatalf("background startup failed: %v", err)
				}
				select {
				case message := <-reported:
					if !strings.Contains(message, failure.Error()) {
						t.Fatalf("background error = %q", message)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("background error not reported")
				}
			}
			if !wait {
				started <- struct{}{}
			}
			select {
			case err := <-runExited:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("run exit = %v, want cancellation", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("failed preload did not stop Run")
			}
			if client.ctx.Err() == nil {
				t.Fatal("failed preload did not cancel client context")
			}
		})
	}
}
