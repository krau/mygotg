package mygotg

import (
	"context"
	"sync"

	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	intErrors "github.com/krau/mygotg/errors"
)

type recoveryAttempt struct {
	ready   chan struct{}
	commit  chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
	started bool
	err     error
}

type updateRecovery struct {
	manager *updates.Manager
	parent  context.Context

	mu             sync.Mutex
	queue          []tg.UpdatesClass
	active         bool
	ctx            context.Context
	managerContext context.Context
	api            updates.API
	userID         int64
	isBot          bool
	attempt        *recoveryAttempt
	onError        func(error)
}

func newUpdateRecovery(manager *updates.Manager, parent context.Context, onError func(error)) *updateRecovery {
	return &updateRecovery{
		manager: manager,
		parent:  parent,
		onError: onError,
	}
}

func (r *updateRecovery) Handle(ctx context.Context, update tg.UpdatesClass) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.parent.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.active {
		runCtx := r.managerContext
		r.mu.Unlock()
		if err := runCtx.Err(); err != nil {
			return err
		}
		return r.manager.Handle(ctx, update)
	}
	r.queue = append(r.queue, update)
	r.mu.Unlock()
	return nil
}

func (r *updateRecovery) bind(ctx context.Context, api updates.API, userID int64, isBot bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ctx, r.api, r.userID, r.isBot = ctx, api, userID, isBot
}

func (r *updateRecovery) start(ctx context.Context) error {
	r.mu.Lock()
	if r.ctx == nil || r.ctx.Err() != nil || r.parent.Err() != nil {
		r.mu.Unlock()
		return intErrors.ErrClientNotReady
	}
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return err
	}
	if r.attempt != nil {
		r.mu.Unlock()
		return intErrors.ErrUpdateRecoveryStarted
	}
	runCtx, cancel := context.WithCancel(r.ctx)
	attempt := &recoveryAttempt{
		ready: make(chan struct{}), commit: make(chan struct{}), done: make(chan struct{}), cancel: cancel,
	}
	r.attempt = attempt
	r.managerContext = runCtx
	api, userID, isBot := r.api, r.userID, r.isBot
	r.mu.Unlock()
	go r.run(runCtx, api, userID, isBot, attempt)

	select {
	case <-attempt.ready:
		r.mu.Lock()
		err := ctx.Err()
		if err == nil {
			err = runCtx.Err()
		}
		if err == nil {
			attempt.started = true
			close(attempt.commit)
		}
		r.mu.Unlock()
		if err != nil {
			cancel()
			<-attempt.done
		}
		return err
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		cancel()
		<-attempt.done
		return ctx.Err()
	case <-runCtx.Done():
		cancel()
		<-attempt.done
		if err := ctx.Err(); err != nil {
			return err
		}
		return attempt.err
	}
}

func (r *updateRecovery) run(ctx context.Context, api updates.API, userID int64, isBot bool, attempt *recoveryAttempt) {
	var forwardDone chan struct{}
	err := r.manager.Run(ctx, api, userID, updates.AuthOptions{
		IsBot: isBot,
		OnStart: func(ctx context.Context) {
			close(attempt.ready)
			select {
			case <-attempt.commit:
				forwardDone = make(chan struct{})
				go func() {
					defer close(forwardDone)
					r.forward(ctx)
				}()
			case <-ctx.Done():
			}
		},
	})
	attempt.cancel()
	if forwardDone != nil {
		<-forwardDone
	}
	r.mu.Lock()
	attempt.err = err
	started := attempt.started
	if !started {
		r.manager.Reset()
		if r.attempt == attempt {
			r.attempt = nil
		}
	}
	r.mu.Unlock()
	if started && r.ctx.Err() == nil && r.parent.Err() == nil && r.onError != nil {
		r.onError(err)
	}
	close(attempt.done)
}

func (r *updateRecovery) forward(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		r.mu.Lock()
		queued := r.queue
		r.queue = nil
		if len(queued) == 0 {
			r.active = true
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		for i, update := range queued {
			queued[i] = nil
			if err := r.manager.Handle(ctx, update); err != nil {
				return
			}
		}
	}
}

func (r *updateRecovery) stop() {
	r.mu.Lock()
	r.queue = nil
	attempt := r.attempt
	if attempt != nil {
		attempt.cancel()
	}
	r.mu.Unlock()
	if attempt != nil {
		<-attempt.done
	}
}

// A deferred first start needs the login-time baseline, not a later cursor that already includes queued updates.
type recoveryAPI struct {
	updates.API
	initial tg.UpdatesState
}

func (a recoveryAPI) UpdatesGetState(ctx context.Context) (*tg.UpdatesState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	state := a.initial
	return &state, nil
}
