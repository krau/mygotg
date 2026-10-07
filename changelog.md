# Changelog

mygotg is derived from [gotgproto](https://github.com/celestix/gotgproto); the history up to
the fork lives in gotgproto's changelog. This file tracks changes made in this repository.

## Unreleased

### Fixed

- Channel and user access hashes persist across restarts without clearing peer usernames.
- Hash lookups report cancellation and storage failures; missing or zero hashes remain unknown.
- Older queued peer writes cannot overwrite a successful newer hash or preload save.
- Saving partial users or channels does not replace complete peer records.

### Compatibility

- Public APIs and the peer database schema are unchanged; no database migration is required.
- `AddPeer` still publishes peers before returning and saves asynchronously, but peer mutations
  can wait for a concurrent storage operation. Hash callers must handle returned storage errors.

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
