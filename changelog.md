# Changelog

mygotg is derived from [gotgproto](https://github.com/celestix/gotgproto); the history up to
the fork lives in gotgproto's changelog. This file tracks changes made in this repository.

## v0.5.0 - 2026-10-08

### Added

- `Context.ResolveInputPeerById` and `Context.ResolvePeerById` resolve peers missing from the
  peer storage over the network: channels via `channels.getChannels` and users via
  `users.getUsers` with a zero access hash, chats are assumed to exist. Both plain peer ids and
  Bot API style marked ids (`-100<channel>`, `-<chat>`) are accepted.

### Changed

- Find and send helpers (`GetChat`, `GetUser`, `BanChatMember`, `ForwardMessages`, `SendMessage`,
  `EditMessage`, …) resolve unknown peers with the new resolver instead of failing immediately
  or sending a nil peer.

## v0.4.0 - 2026-10-07

### Added

- `ClientOpts.DeferUpdateRecovery` and `Client.StartUpdateRecovery(ctx)` let applications
  register handlers before buffered updates and missed updates are delivered.

### Fixed

- Channel and user access hashes persist across restarts without clearing peer usernames.
- Hash lookups report cancellation and storage failures; missing or zero hashes remain unknown.
- Older queued peer writes cannot overwrite a successful newer hash or preload save.
- Saving partial users or channels does not replace complete peer records.
- Recovery stops with its client run; canceled or failed initialization can be retried.
- Stopped or unsuccessfully restarted clients cannot start recovery using stale login state.
- Client Stop/Start no longer repeats the dispatcher initialization notification.

### Compatibility

- Existing public signatures and the peer database schema are unchanged; no migration is required.
- `AddPeer` still publishes peers before returning and saves asynchronously, but peer mutations
  can wait for a concurrent storage operation. Hash callers must handle returned storage errors.
- Persistent peer cache entries use a fixed six-hour TTL instead of renewal on reads to avoid
  a cache expiry race; expired entries reload from the database.
- Automatic recovery remains the default, but `NewClient`/`Start` wait for recovery initialization
  and return initialization errors. Deferred startup also reports initial update-state fetch errors.
- Register every handler before `StartUpdateRecovery` and check its returned error. Its context
  limits initialization only; after success, stop with `Client.Stop()` or `ClientOpts.Context`.
- Deferred updates wait in memory; start recovery promptly. Use `NoUpdates` for clients that never
  consume updates, and do not share recovery cursors between independent consumers.
- See [handler registration and migration guidance](README.md#registering-handlers-before-recovery).

## v0.3.0 - 2026-10-06

### Added

- Missed-update recovery, with optional persistence through `ClientOpts.UpdateStateStorage`.
- reCAPTCHA support through `RecaptchaSolver`, and code delivery information in
  `AuthStatus.SentCodeType`.
- Startup peer preloading through `ClientOpts.PeersFromDialogs` and
  `WaitOnPeersFromDialogs`.
- golangci-lint v2 configuration and a repository changelog.

### Fixed

- Session exports reflect the latest stored session; caller buffer changes cannot alter it.
- Peer preloading preserves complete users and channels, reports failures, and makes
  persisted peers available before a waiting startup returns.
- Failed peer preloading can stop startup without a duplicate completion notification.
- Empty administrator titles continue to clear existing titles after the gotd upgrade.

### Changed

- Updated `github.com/gotd/td` to v0.159.0.
- The reported client version now matches this repository's release tag.

## v0.2.1 - 2026-02-11

- Added SQLite migration for existing peer databases with a single-column primary key.

## v0.2.0 - 2026-02-11

- Changed peer storage to use a combined ID and entity-type primary key.
