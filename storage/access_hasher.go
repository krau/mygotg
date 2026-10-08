package storage

import (
	"context"

	"github.com/gotd/td/telegram/updates"
)

// AccessHasher adapts the peer storage to gotd's updates.ChannelAccessHasher
// and updates.UserAccessHasher, so the update manager keeps the access hashes it
// learns in the same persistent store as the other peers.
//
// Without it the manager falls back to an in-memory hasher: after a restart it
// cannot resolve the access hash of a channel it already knows the pts of, so
// that channel's state is skipped and its gap (updatesTooLong) is ignored.
type AccessHasher struct {
	peers *PeerStorage
}

var (
	_ updates.ChannelAccessHasher = (*AccessHasher)(nil)
	_ updates.UserAccessHasher    = (*AccessHasher)(nil)
)

// NewAccessHasher returns an access hasher backed by the given peer storage.
func NewAccessHasher(peers *PeerStorage) *AccessHasher {
	return &AccessHasher{peers: peers}
}

// SetChannelAccessHash implements updates.ChannelAccessHasher.
func (h *AccessHasher) SetChannelAccessHash(ctx context.Context, _, channelID, accessHash int64) error {
	return h.upsert(ctx, channelID, accessHash, TypeChannel)
}

// GetChannelAccessHash implements updates.ChannelAccessHasher.
func (h *AccessHasher) GetChannelAccessHash(ctx context.Context, _, channelID int64) (int64, bool, error) {
	return h.lookup(ctx, channelID, TypeChannel)
}

// InvalidateChannelAccessHash clears a rejected hash without discarding newer metadata or the username.
func (h *AccessHasher) InvalidateChannelAccessHash(ctx context.Context, channelID, rejectedHash int64) error {
	h.peers.peerLock.Lock()
	defer h.peers.peerLock.Unlock()
	peer, found, err := h.peers.getPeerByIDTypeLocked(ctx, channelID, TypeChannel)
	if err != nil {
		return err
	}
	if !found || peer.AccessHash != rejectedHash {
		return nil
	}
	invalidated := *peer
	invalidated.AccessHash = 0
	return h.peers.savePeerLocked(ctx, &invalidated)
}

// SetUserAccessHash implements updates.UserAccessHasher.
func (h *AccessHasher) SetUserAccessHash(ctx context.Context, _, targetUserID, accessHash int64) error {
	return h.upsert(ctx, targetUserID, accessHash, TypeUser)
}

// GetUserAccessHash implements updates.UserAccessHasher.
func (h *AccessHasher) GetUserAccessHash(ctx context.Context, _, targetUserID int64) (int64, bool, error) {
	return h.lookup(ctx, targetUserID, TypeUser)
}

// upsert preserves the username atomically with the hash write.
func (h *AccessHasher) upsert(ctx context.Context, id, accessHash int64, peerType EntityType) error {
	h.peers.peerLock.Lock()
	defer h.peers.peerLock.Unlock()
	existing, found, err := h.peers.getPeerByIDTypeLocked(ctx, id, peerType)
	if err != nil {
		return err
	}
	peer := &Peer{ID: id, AccessHash: accessHash, Type: peerType.GetInt()}
	if found {
		peer.Username = existing.Username
	}
	return h.peers.savePeerLocked(ctx, peer)
}

func (h *AccessHasher) lookup(ctx context.Context, id int64, peerType EntityType) (int64, bool, error) {
	peer, found, err := h.peers.getPeerByIDType(ctx, id, peerType)
	if err != nil {
		return 0, false, err
	}
	if !found || peer.AccessHash == 0 {
		return 0, false, nil
	}
	return peer.AccessHash, true, nil
}
