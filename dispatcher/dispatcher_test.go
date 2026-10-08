package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/krau/mygotg/ext"
)

type dispatcherTestHandler func(*ext.Context, *ext.Update) error

func (h dispatcherTestHandler) CheckUpdate(c *ext.Context, u *ext.Update) error {
	return h(c, u)
}

func newTestDispatcher(t *testing.T, onError ErrorHandler, cancel context.CancelFunc) *NativeDispatcher {
	t.Helper()
	dp := NewNativeDispatcher(false, false, onError, nil, nil)
	dp.Initialize(context.Background(), cancel, telegram.NewClient(1, "test", telegram.Options{}), &tg.User{ID: 1})
	return dp
}

func TestNativeDispatcherGroupControl(t *testing.T) {
	for _, fromErrorHandler := range []bool{false, true} {
		for _, wrapped := range []bool{false, true} {
			for _, tc := range []struct {
				name   string
				result error
				want   []string
				stop   bool
			}{
				{name: "nil", want: []string{"first", "control", "same", "last"}},
				{name: "continue", result: ContinueGroups, want: []string{"first", "control", "same", "last"}},
				{name: "skip", result: SkipCurrentGroup, want: []string{"first", "control", "last"}},
				{name: "end", result: EndGroups, want: []string{"first", "control"}},
				{name: "stop", result: StopClient, want: []string{"first", "control"}, stop: true},
			} {
				t.Run(fmt.Sprintf("%s/error-handler=%t/wrapped=%t", tc.name, fromErrorHandler, wrapped), func(t *testing.T) {
					result := tc.result
					if wrapped && result != nil {
						result = fmt.Errorf("wrapped control: %w", result)
					}
					failure := errors.New("handler failure")
					errorCalls, cancelCalls := 0, 0
					dp := newTestDispatcher(t, func(_ *ext.Context, _ *ext.Update, message string) error {
						errorCalls++
						if message != failure.Error() {
							t.Fatalf("error message = %q, want %q", message, failure.Error())
						}
						return result
					}, func() { cancelCalls++ })
					var called []string
					add := func(group int, name string, err error) {
						dp.AddHandlerToGroup(dispatcherTestHandler(func(_ *ext.Context, _ *ext.Update) error {
							called = append(called, name)
							return err
						}), group)
					}
					add(20, "last", nil)
					trigger := result
					if fromErrorHandler {
						trigger = failure
					}
					add(10, "control", trigger)
					add(10, "same", nil)
					add(-1, "first", nil)
					if err := dp.Handle(context.Background(), &tg.UpdateShort{Update: &tg.UpdateUserStatus{UserID: 2}}); err != nil {
						t.Fatalf("control escaped Handle: %v", err)
					}
					if !reflect.DeepEqual(called, tc.want) {
						t.Fatalf("handlers = %v, want %v", called, tc.want)
					}
					wantErrorCalls := 0
					if fromErrorHandler {
						wantErrorCalls = 1
					}
					if errorCalls != wantErrorCalls {
						t.Fatalf("error handler calls = %d, want %d", errorCalls, wantErrorCalls)
					}
					wantCancelCalls := 0
					if tc.stop {
						wantCancelCalls = 1
					}
					if cancelCalls != wantCancelCalls {
						t.Fatalf("cancel calls = %d, want %d", cancelCalls, wantCancelCalls)
					}
				})
			}
		}
	}
}

func TestNativeDispatcherUnhandledError(t *testing.T) {
	failure := errors.New("handler failure")
	unhandled := errors.New("unhandled failure")
	returned := fmt.Errorf("error handler: %w", unhandled)
	errorCalls := 0
	dp := newTestDispatcher(t, func(_ *ext.Context, _ *ext.Update, message string) error {
		errorCalls++
		if message != failure.Error() {
			t.Fatalf("error message = %q, want %q", message, failure.Error())
		}
		return returned
	}, func() { t.Fatal("unexpected cancellation") })
	dp.AddHandler(dispatcherTestHandler(func(_ *ext.Context, _ *ext.Update) error { return failure }))
	dp.AddHandler(dispatcherTestHandler(func(_ *ext.Context, _ *ext.Update) error {
		t.Fatal("later success must not overwrite an unhandled error")
		return nil
	}))
	dp.AddHandlerToGroup(dispatcherTestHandler(func(_ *ext.Context, _ *ext.Update) error {
		t.Fatal("later groups must not overwrite an unhandled error")
		return nil
	}), 1)
	err := dp.Handle(context.Background(), &tg.UpdateShort{Update: &tg.UpdateUserStatus{UserID: 2}})
	if err != returned || !errors.Is(err, unhandled) {
		t.Fatalf("Handle error = %v, want %v", err, returned)
	}
	if errorCalls != 1 {
		t.Fatalf("error handler calls = %d, want 1", errorCalls)
	}
}

func TestNativeDispatcherBatchPreservesFailures(t *testing.T) {
	first := errors.New("first failure")
	second := errors.New("second failure")
	for _, combined := range []bool{false, true} {
		t.Run(fmt.Sprintf("combined=%t", combined), func(t *testing.T) {
			errorCalls := 0
			dp := newTestDispatcher(t, func(_ *ext.Context, u *ext.Update, _ string) error {
				errorCalls++
				if u.UpdateClass.(*tg.UpdateUserStatus).UserID == 1 {
					return first
				}
				return fmt.Errorf("wrapped failure: %w", second)
			}, func() { t.Fatal("unexpected cancellation") })
			var seen []int64
			dp.AddHandler(dispatcherTestHandler(func(_ *ext.Context, u *ext.Update) error {
				id := u.UpdateClass.(*tg.UpdateUserStatus).UserID
				seen = append(seen, id)
				if id < 3 {
					return errors.New("handler failure")
				}
				return ContinueGroups
			}))
			upds := []tg.UpdateClass{
				&tg.UpdateUserStatus{UserID: 1},
				&tg.UpdateUserStatus{UserID: 2},
				&tg.UpdateUserStatus{UserID: 3},
			}
			var batch tg.UpdatesClass = &tg.Updates{Updates: upds}
			if combined {
				batch = &tg.UpdatesCombined{Updates: upds}
			}
			err := dp.Handle(context.Background(), batch)
			if !errors.Is(err, first) || !errors.Is(err, second) {
				t.Fatalf("batch lost errors: %v", err)
			}
			if errors.Is(err, ContinueGroups) {
				t.Fatalf("batch leaked control signal: %v", err)
			}
			if !reflect.DeepEqual(seen, []int64{1, 2, 3}) || errorCalls != 2 {
				t.Fatalf("seen = %v, error calls = %d", seen, errorCalls)
			}
		})
	}
}
