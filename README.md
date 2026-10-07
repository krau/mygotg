# mygotg

hard fork of gotgproto

## Registering handlers before recovery

Clients that register handlers after `NewClient` should set `ClientOpts.DeferUpdateRecovery`.
After registering every handler, start recovery and check the result:

```go
client, err := mygotg.NewClient(appID, apiHash, clientType, &mygotg.ClientOpts{
	Session:             sessionConstructor,
	DeferUpdateRecovery: true,
})
if err != nil {
	return err
}
defer client.Stop()
client.Dispatcher.AddHandler(messageHandler)
if err := client.StartUpdateRecovery(ctx); err != nil {
	return err
}
return client.Idle()
```

- `ctx` limits recovery initialization. Canceling it after success does not stop the client;
  use `client.Stop()` or cancel `ClientOpts.Context` for shutdown.
- Initialization failures can be retried while the client is running. A second successful
  start returns `ErrUpdateRecoveryStarted`; a stopped client returns `ErrClientNotReady`.
- Updates received before recovery starts wait in memory. Start promptly after registration.
  Use `NoUpdates: true` for takeout/export clients that never consume updates, and do not
  share a recovery cursor between independent update consumers.
- Omitting `DeferUpdateRecovery` keeps automatic recovery. Automatic initialization failures
  now return from `NewClient`/`Start` instead of leaving a client without working recovery.
