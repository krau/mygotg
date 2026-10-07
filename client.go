package mygotg

//go:generate go run ./generator

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	gotdlog "github.com/gotd/log"
	"github.com/gotd/log/logzap"
	tdsession "github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	"github.com/krau/mygotg/dispatcher"
	intErrors "github.com/krau/mygotg/errors"
	"github.com/krau/mygotg/ext"
	"github.com/krau/mygotg/functions"
	"github.com/krau/mygotg/session"
	"github.com/krau/mygotg/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const VERSION = "v0.3.0"

type Client struct {
	// Dispatcher handlers the incoming updates and execute mapped handlers. It is recommended to use dispatcher.MakeDispatcher function for this field.
	Dispatcher dispatcher.Dispatcher
	// updateManager recovers missed updates (gaps) and hands updates to
	// Dispatcher off the connection's read goroutine. It is nil when updates
	// are disabled or recovery is turned off.
	updateManager    *updates.Manager
	updateRecovery   *updateRecovery
	updateRecoveryMu sync.Mutex
	runContext       context.Context
	// PublicKeys of telegram.
	//
	// If not provided, embedded public keys will be used.
	PublicKeys []telegram.PublicKey
	// DC ID to connect.
	//
	// If not provided, 2 will be used by default.
	DC int
	// DCList is initial list of addresses to connect.
	DCList dcs.List
	// Resolver to use.
	Resolver dcs.Resolver
	// MigrationTimeout configures migration timeout.
	MigrationTimeout time.Duration
	// AckBatchSize is limit of MTProto ACK buffer size.
	AckBatchSize int
	// AckInterval is maximum time to buffer MTProto ACK.
	AckInterval time.Duration
	// RetryInterval is duration between send retries.
	RetryInterval time.Duration
	// MaxRetries is limit of send retries.
	MaxRetries int
	// ExchangeTimeout is timeout of every key exchange request.
	ExchangeTimeout time.Duration
	// DialTimeout is timeout of creating connection.
	DialTimeout time.Duration
	// CompressThreshold is a threshold in bytes to determine that message
	// is large enough to be compressed using GZIP.
	// If < 0, compression will be disabled.
	// If == 0, default value will be used.
	CompressThreshold int
	// Whether to show the copyright line in console or no.
	DisableCopyright bool
	// Logger is instance of zap.Logger. No logs by default.
	Logger *zap.Logger
	// Session info of the authenticated user, use session.NewSession function to fill this field.
	sessionStorage tdsession.Storage
	// Self contains details of logged in user in the form of *tg.User.
	Self *tg.User
	// Code for the language used on the device's OS, ISO 639-1 standard.
	SystemLangCode string
	// Code for the language used on the client, ISO 639-1 standard.
	ClientLangCode string
	// PeerStorage is the storage for all the peers.
	// It is recommended to use storage.NewPeerStorage function for this field.
	PeerStorage *storage.PeerStorage
	// NoAutoAuth is a flag to disable automatic authentication
	// if the current session is invalid.
	NoAutoAuth bool
	// NoUpdates is a flag to disable updates.
	NoUpdates bool

	authConversator AuthConversator
	sendCodeOptions auth.SendCodeOptions
	clientType      clientType
	ctx             context.Context
	err             error
	autoFetchReply  bool
	cancel          context.CancelFunc
	running         bool
	*telegram.Client
	appId        int
	apiHash      string
	deviceParams tg.JSONValueClass
}

type ClientOpts struct {
	// Logger is instance of zap.Logger. No logs by default.
	Logger *zap.Logger
	// Whether to store session and peer storage in memory or not
	//
	// Note: Sessions and Peers won't be persistent if this field is set to true.
	InMemory bool
	// PublicKeys of telegram.
	//
	// If not provided, embedded public keys will be used.
	PublicKeys []telegram.PublicKey
	// DC ID to connect.
	//
	// If not provided, 2 will be used by default.
	DC int
	// DCList is initial list of addresses to connect.
	DCList dcs.List
	// Resolver to use.
	Resolver dcs.Resolver
	// Whether to show the copyright line in console or no.
	DisableCopyright bool
	// Session info of the authenticated user, use session.NewSession function to fill this field.
	Session session.SessionConstructor
	// Setting this field to true will lead to automatically fetch the reply_to_message for a new message update.
	//
	// Set to `false` by default.
	AutoFetchReply bool
	// Setting this field to true will lead to automatically fetch the entire reply_to_message chain for a new message update.
	//
	// Set to `false` by default.
	FetchEntireReplyChain bool
	// Code for the language used on the device's OS, ISO 639-1 standard.
	SystemLangCode string
	// Code for the language used on the client, ISO 639-1 standard.
	ClientLangCode string
	// Custom client device
	Device *telegram.DeviceConfig
	// Panic handles all the panics that occur during handler execution.
	PanicHandler dispatcher.PanicHandler
	// ErrorHandler handles unknown callback errors and background peer-preload failures (with a nil update).
	// Returning dispatcher.StopClient cancels the client.
	ErrorHandler dispatcher.ErrorHandler
	// Custom Middlewares
	Middlewares []telegram.Middleware
	// UpdateStateStorage persists the update manager state (pts/qts/seq and
	// per-channel pts), so updates missed while the client was offline are
	// recovered via updates.getDifference on the next start instead of being
	// lost. In-memory storage is used if not provided.
	UpdateStateStorage updates.StateStorage
	// DisableUpdateRecovery turns off missed-update (gap) recovery.
	//
	// With it, updates are handled as-is and anything missed after a reconnect
	// or an updatesTooLong is lost, which is the historic reason a long-running
	// client silently stops responding. Only enable this for debugging.
	DisableUpdateRecovery bool
	// DeferUpdateRecovery buffers updates until StartUpdateRecovery is called after handler registration.
	// No update cursor is persisted before recovery starts.
	// Requires update recovery; use NoUpdates for clients that never consume updates.
	DeferUpdateRecovery bool
	// Custom Run() Middleware
	// Can be used for floodWaiter package
	// https://github.com/krau/mygotg/blob/beta/examples/middleware/main.go#L41
	RunMiddleware func(
		origRun func(ctx context.Context, f func(ctx context.Context) error) (err error),
		ctx context.Context,
		f func(ctx context.Context) (err error),
	) (err error)
	// A custom context to use for the client.
	// If not provided, context.Background() will be used.
	// Note: This context will be used for the entire lifecycle of the client.
	Context context.Context
	// AuthConversator is the interface for the authenticator.
	// mygotg.BasicConversator is used by default.
	AuthConversator AuthConversator
	// MigrationTimeout configures migration timeout.
	MigrationTimeout time.Duration
	// AckBatchSize is limit of MTProto ACK buffer size.
	AckBatchSize int
	// AckInterval is maximum time to buffer MTProto ACK.
	AckInterval time.Duration
	// RetryInterval is duration between send retries.
	RetryInterval time.Duration
	// MaxRetries is limit of send retries.
	MaxRetries int
	// ExchangeTimeout is timeout of every key exchange request.
	ExchangeTimeout time.Duration
	// DialTimeout is timeout of creating connection.
	DialTimeout time.Duration
	// CompressThreshold is a threshold in bytes to determine that message
	// is large enough to be compressed using GZIP.
	// If < 0, compression will be disabled.
	// If == 0, default value will be used.
	CompressThreshold int
	// NoAutoAuth is a flag to disable automatic authentication
	// if the current session is invalid.
	NoAutoAuth bool
	// NoUpdates is a flag to disable updates.
	NoUpdates bool
	// SendCodeOptions allows overriding AuthSendCode behavior.
	SendCodeOptions *auth.SendCodeOptions
	// PeersFromDialogs preloads user-account dialogs; background failures use ErrorHandler or logging.
	PeersFromDialogs bool
	// WaitOnPeersFromDialogs waits for persistence; failure cancels startup and is returned to the caller.
	WaitOnPeersFromDialogs bool
}

// NewClient creates a new mygotg client and logs in to telegram.
func NewClient(appId int, apiHash string, cType clientType, opts *ClientOpts) (*Client, error) {
	if opts == nil {
		opts = &ClientOpts{
			SystemLangCode: "en",
			ClientLangCode: "en",
		}
	}

	if opts.Context == nil {
		opts.Context = context.Background()
	}
	ctx, cancel := context.WithCancel(opts.Context)

	peerStorage, sessionStorage, err := session.NewSessionStorage(ctx, opts.Session, opts.InMemory)
	if err != nil {
		cancel()
		return nil, err
	}

	if opts.AuthConversator == nil {
		opts.AuthConversator = BasicConversator()
	}

	d := dispatcher.NewNativeDispatcher(opts.AutoFetchReply, opts.FetchEntireReplyChain, opts.ErrorHandler, opts.PanicHandler, peerStorage)

	c := Client{
		Resolver:          opts.Resolver,
		PublicKeys:        opts.PublicKeys,
		DC:                opts.DC,
		DCList:            opts.DCList,
		MigrationTimeout:  opts.MigrationTimeout,
		AckBatchSize:      opts.AckBatchSize,
		AckInterval:       opts.AckInterval,
		RetryInterval:     opts.RetryInterval,
		MaxRetries:        opts.MaxRetries,
		ExchangeTimeout:   opts.ExchangeTimeout,
		DialTimeout:       opts.DialTimeout,
		CompressThreshold: opts.CompressThreshold,
		DisableCopyright:  opts.DisableCopyright,
		Logger:            opts.Logger,
		SystemLangCode:    opts.SystemLangCode,
		ClientLangCode:    opts.ClientLangCode,
		NoAutoAuth:        opts.NoAutoAuth,
		NoUpdates:         opts.NoUpdates,
		authConversator:   opts.AuthConversator,
		Dispatcher:        d,
		PeerStorage:       peerStorage,
		sessionStorage:    sessionStorage,
		clientType:        cType,
		ctx:               ctx,
		autoFetchReply:    opts.AutoFetchReply,
		cancel:            cancel,
		appId:             appId,
		apiHash:           apiHash,
	}
	if opts.SendCodeOptions != nil {
		c.sendCodeOptions = *opts.SendCodeOptions
	}
	c.printCredit()

	return &c, c.Start(opts)
}

func (c *Client) updateHandler() telegram.UpdateHandler {
	if c.updateRecovery != nil {
		return c.updateRecovery
	}
	return c.Dispatcher
}

func gotdLogger(logger *zap.Logger) gotdlog.Logger {
	if logger == nil {
		return nil
	}
	return logzap.New(logger)
}

func (c *Client) initTelegramClient(
	device *telegram.DeviceConfig,
	middlewares []telegram.Middleware,
) {
	if device == nil {
		device = &telegram.DeviceConfig{
			DeviceModel:    "mygotg",
			SystemVersion:  runtime.GOOS,
			AppVersion:     VERSION,
			SystemLangCode: c.SystemLangCode,
			LangCode:       c.ClientLangCode,
		}
	}
	c.deviceParams = device.Params
	c.Client = telegram.NewClient(c.appId, c.apiHash, telegram.Options{
		DCList:            c.DCList,
		Resolver:          c.Resolver,
		DC:                c.DC,
		PublicKeys:        c.PublicKeys,
		MigrationTimeout:  c.MigrationTimeout,
		AckBatchSize:      c.AckBatchSize,
		AckInterval:       c.AckInterval,
		RetryInterval:     c.RetryInterval,
		MaxRetries:        c.MaxRetries,
		ExchangeTimeout:   c.ExchangeTimeout,
		DialTimeout:       c.DialTimeout,
		CompressThreshold: c.CompressThreshold,
		UpdateHandler:     c.updateHandler(),
		NoUpdates:         c.NoUpdates,
		SessionStorage:    c.sessionStorage,
		Logger:            gotdLogger(c.Logger),
		Device:            *device,
		Middlewares:       middlewares,
	})
}

func (c *Client) login() error {
	authClient := c.Auth()
	status, err := authClient.Status(c.ctx)
	if err != nil {
		return errors.Wrap(err, "auth status")
	}
	if status.Authorized {
		return nil
	}
	if c.clientType.getType() == clientTypeVPhone {
		if c.NoAutoAuth {
			return intErrors.ErrSessionUnauthorized
		}
		var flowClient auth.FlowClient = authClient
		if solver, ok := c.authConversator.(RecaptchaSolver); ok {
			flowClient = FlowClient{
				FlowClient: authClient,
				api:        c.API(),
				appID:      c.appId,
				apiHash:    c.apiHash,
				params:     c.deviceParams,
				solver:     solver,
			}
		}
		err = authFlow(
			c.ctx, flowClient,
			c.authConversator,
			c.clientType.getValue(),
			c.sendCodeOptions,
		)
		if err != nil {
			return errors.Wrap(err, "auth flow")
		}
	} else {
		if !status.Authorized {
			if _, err := c.Auth().Bot(c.ctx, c.clientType.getValue()); err != nil {
				return errors.Wrap(err, "login")
			}
		}
	}
	return nil
}

func (ch *Client) printCredit() {
	if !ch.DisableCopyright {
		fmt.Printf(`
mygotg %s, Copyright (C) 2024 Anony <github.com/celestix>
Licensed under the terms of GNU General Public License v3

`, VERSION)
	}
}

func (c *Client) initialize(notifyStarted func(error), recovery *updateRecovery, deferRecovery bool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		err := c.login()
		if err != nil {
			return err
		}
		self, err := c.Client.Self(ctx)
		if err != nil {
			return err
		}

		c.Self = self

		c.Dispatcher.Initialize(ctx, c.Stop, c.Client, self)

		if recovery != nil {
			var api updates.API = c.API()
			if deferRecovery {
				state, err := api.UpdatesGetState(ctx)
				if err != nil {
					return errors.Wrap(err, "get initial update state")
				}
				api = recoveryAPI{API: api, initial: *state}
			}
			recovery.bind(ctx, api, self.ID, c.clientType.getType() != clientTypeVPhone)
			defer recovery.stop()
			if !deferRecovery {
				if err := recovery.start(ctx); err != nil {
					return errors.Wrap(err, "start update recovery")
				}
			}
		}

		c.PeerStorage.AddPeer(self.ID, self.AccessHash, storage.TypeUser, self.Username)
		c.updateRecoveryMu.Lock()
		c.running = true
		c.runContext = ctx
		c.updateRecoveryMu.Unlock()
		defer func() {
			c.updateRecoveryMu.Lock()
			if c.runContext == ctx {
				c.runContext = nil
				c.running = false
			}
			c.updateRecoveryMu.Unlock()
		}()
		notifyStarted(nil)
		<-ctx.Done()
		return ctx.Err()
	}
}

// StartUpdateRecovery waits for recovery initialization after handlers have been registered.
// The context limits this call; after success, recovery runs until the client stops.
// Initialization failures leave recovery available for another start attempt.
// It returns ErrClientNotReady for stopped/uninitialized clients, ErrUpdateRecoveryOff when disabled,
// and ErrUpdateRecoveryStarted for an in-progress or completed start.
func (c *Client) StartUpdateRecovery(ctx context.Context) error {
	c.updateRecoveryMu.Lock()
	if !c.running || c.runContext == nil || c.runContext.Err() != nil || c.ctx.Err() != nil {
		c.updateRecoveryMu.Unlock()
		return intErrors.ErrClientNotReady
	}
	recovery := c.updateRecovery
	c.updateRecoveryMu.Unlock()
	if recovery == nil {
		return intErrors.ErrUpdateRecoveryOff
	}
	return recovery.start(ctx)
}

// ExportStringSession EncodeSessionToString encodes the client session to a string in base64.
//
// Note: You must not share this string with anyone, it contains auth details for your logged in account.
func (c *Client) ExportStringSession() (string, error) {
	// InMemorySession case
	loadedSessionData, err := c.sessionStorage.LoadSession(c.ctx)
	if err == nil {
		loadedSession := &storage.Session{
			Version: storage.LatestVersion,
			Data:    loadedSessionData,
		}
		return functions.EncodeSessionToString(loadedSession)
	}
	return functions.EncodeSessionToString(c.PeerStorage.GetSession())
}

// Idle keeps the current goroutined blocked until the client is stopped.
func (c *Client) Idle() error {
	<-c.ctx.Done()
	c.updateRecoveryMu.Lock()
	defer c.updateRecoveryMu.Unlock()
	return c.err
}

// CreateContext creates a new pseudo updates context.
// A context retrieved from this method should be reused.
func (c *Client) CreateContext() *ext.Context {
	return ext.NewContext(
		c.ctx,
		c.API(),
		c.PeerStorage,
		c.Self,
		message.NewSender(c.API()),
		&tg.Entities{
			Users: map[int64]*tg.User{
				c.Self.ID: c.Self,
			},
		},
		c.autoFetchReply,
	)
}

// Stop cancels the context.Context being used for the client
// and stops it.
//
// Notes:
//
// 1.) Client.Idle() will exit if this method is called.
//
// 2.) You can call Client.Start() to start the client again
// if it was stopped using this method.
func (c *Client) Stop() {
	c.updateRecoveryMu.Lock()
	c.cancel()
	c.running = false
	c.runContext = nil
	c.updateRecoveryMu.Unlock()
}

// Start connects the client to telegram servers and logins.
// It returns an error if already running or a synchronous peer preload fails; a failed preload stops the client.
func (c *Client) Start(opts *ClientOpts) error {
	c.updateRecoveryMu.Lock()
	if c.running {
		c.updateRecoveryMu.Unlock()
		return intErrors.ErrClientAlreadyRunning
	}
	c.runContext = nil
	c.updateRecoveryMu.Unlock()
	if c.updateRecovery != nil {
		c.updateRecovery.stop()
	}
	if c.ctx.Err() == context.Canceled {
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}

	c.updateManager = nil
	c.updateRecovery = nil
	if !opts.NoUpdates && !opts.DisableUpdateRecovery {
		c.updateManager = updates.New(updates.Config{
			Handler: c.Dispatcher,
			Storage: opts.UpdateStateStorage,
			// Keep the access hashes the manager learns in the persistent peer
			// storage; otherwise it uses an in-memory hasher and cannot resolve
			// known channels after a restart, silently skipping their gaps.
			AccessHasher:     storage.NewAccessHasher(c.PeerStorage),
			UserAccessHasher: storage.NewAccessHasher(c.PeerStorage),
			Logger:           gotdLogger(opts.Logger),
		})
		c.updateRecovery = newUpdateRecovery(c.updateManager, c.ctx, func(err error) {
			if c.Logger != nil {
				c.Logger.Error("Update manager stopped", zap.Error(err))
			}
			c.Stop()
		})
	}
	c.initTelegramClient(opts.Device, opts.Middlewares)
	recovery := c.updateRecovery
	runCtx := c.ctx
	startup := make(chan error, 1)
	started := sync.Once{}
	notifyStarted := func(err error) { started.Do(func() { startup <- err }) }
	go func() {
		var err error
		if opts.RunMiddleware == nil {
			err = c.Run(runCtx, c.initialize(notifyStarted, recovery, opts.DeferUpdateRecovery))
		} else {
			err = opts.RunMiddleware(c.Run, runCtx, c.initialize(notifyStarted, recovery, opts.DeferUpdateRecovery))
		}
		c.updateRecoveryMu.Lock()
		c.err = err
		c.updateRecoveryMu.Unlock()
		notifyStarted(err)
	}()

	err := <-startup
	if err == nil {
		if !c.Self.Bot && opts.PeersFromDialogs {
			if opts.WaitOnPeersFromDialogs {
				if err := storage.AddPeersFromDialogs(c.ctx, c.API(), c.PeerStorage); err != nil {
					c.Stop()
					return errors.Wrap(err, "preload peers from dialogs")
				}
			} else {
				go func() {
					if err := storage.AddPeersFromDialogs(c.ctx, c.API(), c.PeerStorage); err != nil {
						c.handlePeerPreloadError(opts, err)
					}
				}()
			}
		}
	}
	return err
}

func (c *Client) handlePeerPreloadError(opts *ClientOpts, err error) {
	if errors.Is(err, context.Canceled) && c.ctx.Err() != nil {
		return
	}
	err = errors.Wrap(err, "preload peers from dialogs")
	if opts.ErrorHandler != nil {
		if errors.Is(opts.ErrorHandler(c.CreateContext(), nil, err.Error()), dispatcher.StopClient) {
			c.Stop()
		}
	} else if c.Logger != nil {
		c.Logger.Error("preload peers from dialogs", zap.Error(err))
	} else {
		log.Println(err)
	}
}

// RefreshContext casts the new context.Context and telegram session
// to ext.Context (It may be used after doing Stop and Start calls respectively.)
func (c *Client) RefreshContext(ctx *ext.Context) {
	(*ctx).Context = c.ctx
	(*ctx).Raw = c.API()
}
