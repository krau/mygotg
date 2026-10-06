# Changelog

mygotg is derived from [gotgproto](https://github.com/celestix/gotgproto); the history up to
the fork lives in gotgproto's changelog. This file tracks changes made in this repository.

## Unreleased

### Added

- `ClientOpts.UpdateStateStorage` and missed-update recovery through gotd's `telegram/updates`
  manager, instead of running handlers on the MTProto read goroutine and dropping gaps.
- reCAPTCHA solver support: the `RecaptchaSolver` interface and the `FlowClient` wrapper that
  retries `auth.sendCode` through `invokeWithReCaptcha`; `AuthStatus.SentCodeType` now reports
  the code delivery method chosen by Telegram.
- `ClientOpts.PeersFromDialogs` / `WaitOnPeersFromDialogs` to seed the peer storage from the
  account dialogs on startup.

### Fixed

- session: `StoreSession` keeps the in-memory snapshot in sync and copies the session bytes on
  read and write, so `ExportStringSession` no longer returns a stale session.
- `userclient/plugin` (btts side): see the corresponding repository.

## v1.0.0-beta21

- Peer storage reworked to a combined primary key, with a SQLite migration for existing
  databases.
