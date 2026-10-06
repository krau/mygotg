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
func (h *AccessHasher) GetChannelAccessHash(_ context.Context, _, channelID int64) (int64, bool, error) {
	return h.lookup(channelID, TypeChannel)
}

// SetUserAccessHash implements updates.UserAccessHasher.
func (h *AccessHasher) SetUserAccessHash(ctx context.Context, _, targetUserID, accessHash int64) error {
	return h.upsert(ctx, targetUserID, accessHash, TypeUser)
}

// GetUserAccessHash implements updates.UserAccessHasher.
func (h *AccessHasher) GetUserAccessHash(_ context.Context, _, targetUserID int64) (int64, bool, error) {
	return h.lookup(targetUserID, TypeUser)
}

// upsert writes the hash without clearing the username the peer may already
// have (AddPeer would overwrite it with an empty one).
func (h *AccessHasher) upsert(ctx context.Context, id, accessHash int64, peerType EntityType) error {
	peer := &Peer{ID: id, AccessHash: accessHash, Type: peerType.GetInt()}
	if existing := h.peers.GetPeerByIdAndType(id, peerType); existing != nil && existing.ID == id {
		peer.Username = existing.Username
	}
	return h.peers.savePreloadedPeer(ctx, peer)
}

// lookup reports the stored access hash. GetPeerByIdAndType returns an empty
// peer when nothing is stored, so the zero ID is treated as "not found".
func (h *AccessHasher) lookup(id int64, peerType EntityType) (int64, bool, error) {
	peer := h.peers.GetPeerByIdAndType(id, peerType)
	if peer == nil || peer.ID != id {
		return 0, false, nil
	}
	return peer.AccessHash, true, nil
}
