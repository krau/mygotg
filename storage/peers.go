package storage

import (
	"context"
	"errors"

	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
	"gorm.io/gorm"
)

// [TODO] bot api style peer id
// https://core.telegram.org/type/Peer
type Peer struct {
	ID         int64 `gorm:"primaryKey"`
	AccessHash int64
	Type       int `gorm:"primaryKey"`
	Username   string
}

type PeerKey struct {
	ID   int64
	Type int
}

type EntityType int

func (e EntityType) GetInt() int {
	return int(e)
}

const (
	DefaultUsername   = ""
	DefaultAccessHash = 0
)

const (
	_ EntityType = iota
	TypeUser
	TypeChat
	TypeChannel
)

// AddPeer publishes the peer before returning and persists it asynchronously.
func (p *PeerStorage) AddPeer(iD, accessHash int64, peerType EntityType, userName string) {
	peer := &Peer{ID: iD, AccessHash: accessHash, Type: peerType.GetInt(), Username: userName}
	key := PeerKey{ID: iD, Type: peer.Type}
	p.peerLock.Lock()
	p.peerCache.Set(key, peer)
	if !p.inMemory {
		if p.pendingPeers == nil {
			p.pendingPeers = make(map[PeerKey]*Peer)
		}
		p.pendingPeers[key] = peer
	}
	p.peerLock.Unlock()
	if !p.inMemory {
		go p.addPeerToDb(peer)
	}
}

func (p *PeerStorage) addPeerToDb(peer *Peer) {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	key := PeerKey{ID: peer.ID, Type: peer.Type}
	if p.pendingPeers[key] != peer {
		return
	}
	p.SqlSession.Save(peer)
	delete(p.pendingPeers, key)
}

func (p *PeerStorage) savePreloadedPeer(ctx context.Context, peer *Peer) error {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	return p.savePeerLocked(ctx, peer)
}

func (p *PeerStorage) savePeerLocked(ctx context.Context, peer *Peer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.inMemory {
		if err := p.SqlSession.WithContext(ctx).Save(peer).Error; err != nil {
			return err
		}
	}
	key := PeerKey{ID: peer.ID, Type: peer.Type}
	delete(p.pendingPeers, key)
	p.peerCache.Set(key, peer)
	return nil
}

// GetPeerById finds the provided id in the peer storage and return it if found.
func (p *PeerStorage) GetPeerById(iD int64) *Peer {
	if peer, ok := p.getCachedPeerByID(iD); ok {
		return peer
	}
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	for _, peerType := range peerTypeLookupOrder() {
		peer, found, err := p.getPeerByIDTypeLocked(context.Background(), iD, peerType)
		if err != nil {
			return &Peer{}
		}
		if found {
			return peer
		}
	}
	return &Peer{}
}

// GetPeerByIdAndType finds the provided id and type in the peer storage and returns it if found.
func (p *PeerStorage) GetPeerByIdAndType(iD int64, peerType EntityType) *Peer {
	peer, found, _ := p.getPeerByIDType(context.Background(), iD, peerType)
	if !found {
		return &Peer{}
	}
	return peer
}

// GetPeerByUsername finds the provided username in the peer storage and return it if found.
func (p *PeerStorage) GetPeerByUsername(username string) *Peer {
	if p.inMemory {
		for _, peer := range p.peerCache.GetAll() {
			if peer.Username == username {
				return peer
			}
		}
	} else {
		peer := Peer{}
		p.SqlSession.Where("username = ?", username).Find(&peer)
		return &peer
	}
	return &Peer{}
}

// GetInputPeerById finds the provided id in the peer storage and return its tg.InputPeerClass if found.
func (p *PeerStorage) GetInputPeerById(iD int64) tg.InputPeerClass {
	return getInputPeerFromStoragePeer(p.GetPeerById(iD))
}

// GetInputPeerByUsername finds the provided username in the peer storage and return its tg.InputPeerClass if found.
func (p *PeerStorage) GetInputPeerByUsername(userName string) tg.InputPeerClass {
	return getInputPeerFromStoragePeer(p.GetPeerByUsername(userName))
}

// Cache misses share the mutation lock so an older DB read cannot replace a newer cached peer.
func (p *PeerStorage) getPeerByIDType(ctx context.Context, id int64, peerType EntityType) (*Peer, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if peer, ok := p.peerCache.Get(PeerKey{ID: id, Type: peerType.GetInt()}); ok {
		return peer, true, nil
	}
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	return p.getPeerByIDTypeLocked(ctx, id, peerType)
}

func (p *PeerStorage) getPeerByIDTypeLocked(ctx context.Context, id int64, peerType EntityType) (*Peer, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	key := PeerKey{ID: id, Type: peerType.GetInt()}
	if peer, ok := p.peerCache.Get(key); ok {
		return peer, true, nil
	}
	if peer := p.pendingPeers[key]; peer != nil {
		p.peerCache.Set(key, peer)
		return peer, true, nil
	}
	if p.inMemory {
		return nil, false, nil
	}
	var peer Peer
	err := p.SqlSession.WithContext(ctx).Where("id = ? AND type = ?", id, peerType.GetInt()).First(&peer).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	p.peerCache.Set(key, &peer)
	return &peer, true, nil
}

func (p *PeerStorage) getCachedPeerByID(id int64) (*Peer, bool) {
	for _, peerType := range peerTypeLookupOrder() {
		peer, ok := p.peerCache.Get(PeerKey{ID: id, Type: peerType.GetInt()})
		if ok {
			return peer, true
		}
	}
	return nil, false
}

func peerTypeLookupOrder() []EntityType {
	return []EntityType{TypeChannel, TypeUser, TypeChat}
}

func getInputPeerFromStoragePeer(peer *Peer) tg.InputPeerClass {
	switch EntityType(peer.Type) {
	case TypeUser:
		return &tg.InputPeerUser{
			UserID:     peer.ID,
			AccessHash: peer.AccessHash,
		}
	case TypeChat:
		return &tg.InputPeerChat{
			ChatID: peer.ID,
		}
	case TypeChannel:
		return &tg.InputPeerChannel{
			ChannelID:  peer.ID,
			AccessHash: peer.AccessHash,
		}
	default:
		return &tg.InputPeerEmpty{}
	}
}

// AddPeersFromDialogs preloads user-account peers, skipping min users/channels.
// It waits for persistence and returns RPC, cancellation, or storage errors.
func AddPeersFromDialogs(ctx context.Context, raw *tg.Client, peerStorage *PeerStorage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	const batchSize = 100
	query := dialogs.NewQueryBuilder(raw).GetDialogs()
	// gotd's iterator otherwise requests again after exhausting a final page.
	finalPageRemaining := -1
	iter := dialogs.NewIterator(dialogs.QueryFunc(func(ctx context.Context, req dialogs.Request) (tg.MessagesDialogsClass, error) {
		result, err := query.Query(ctx, req)
		if err != nil {
			return nil, err
		}
		if page, ok := result.(*tg.MessagesDialogs); ok {
			finalPageRemaining = len(page.Dialogs)
		}
		// Persist the page once; each iterator element shares its entities.
		if page, ok := result.AsModified(); ok {
			for _, user := range page.GetUsers() {
				if user, ok := user.AsNotEmpty(); ok && !user.Min {
					if err := peerStorage.savePreloadedPeer(ctx, &Peer{ID: user.ID, AccessHash: user.AccessHash, Type: TypeUser.GetInt(), Username: user.Username}); err != nil {
						return nil, err
					}
				}
			}
			for _, chat := range page.GetChats() {
				var peer *Peer
				switch chat := chat.(type) {
				case *tg.Channel:
					if !chat.Min {
						peer = &Peer{ID: chat.ID, AccessHash: chat.AccessHash, Type: TypeChannel.GetInt(), Username: chat.Username}
					}
				case *tg.Chat:
					peer = &Peer{ID: chat.ID, Type: TypeChat.GetInt()}
				}
				if peer != nil {
					if err := peerStorage.savePreloadedPeer(ctx, peer); err != nil {
						return nil, err
					}
				}
			}
		}
		return result, nil
	}), batchSize)
	for iter.Next(ctx) {
		if finalPageRemaining > 0 {
			finalPageRemaining--
			if finalPageRemaining == 0 {
				break
			}
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return ctx.Err()
}
