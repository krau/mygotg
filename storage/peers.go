package storage

import (
	"context"

	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
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

func (p *PeerStorage) AddPeer(iD, accessHash int64, peerType EntityType, userName string) {
	peer := &Peer{ID: iD, AccessHash: accessHash, Type: peerType.GetInt(), Username: userName}
	key := PeerKey{ID: iD, Type: peer.Type}
	p.peerCache.Set(key, peer)
	if p.inMemory {
		return
	}
	go p.addPeerToDb(peer)
}

func (p *PeerStorage) addPeerToDb(peer *Peer) {
	tx := p.SqlSession.Begin()
	tx.Save(peer)
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	tx.Commit()
}

func (p *PeerStorage) savePreloadedPeer(ctx context.Context, peer *Peer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.inMemory {
		if err := p.SqlSession.WithContext(ctx).Save(peer).Error; err != nil {
			return err
		}
	}
	p.peerCache.Set(PeerKey{ID: peer.ID, Type: peer.Type}, peer)
	return nil
}

// GetPeerById finds the provided id in the peer storage and return it if found.
func (p *PeerStorage) GetPeerById(iD int64) *Peer {
	peer, ok := p.getCachedPeerByID(iD)
	if p.inMemory {
		if !ok {
			return &Peer{}
		}
	} else {
		if !ok {
			return p.cachePeersByID(iD)
		}
	}
	return peer
}

// GetPeerByIdAndType finds the provided id and type in the peer storage and returns it if found.
func (p *PeerStorage) GetPeerByIdAndType(iD int64, peerType EntityType) *Peer {
	key := PeerKey{ID: iD, Type: peerType.GetInt()}
	peer, ok := p.peerCache.Get(key)
	if p.inMemory {
		if !ok {
			return &Peer{}
		}
		return peer
	}
	if !ok {
		peer, _ := p.cachePeerByIDType(iD, peerType)
		return peer
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

func (p *PeerStorage) cachePeersByID(id int64) *Peer {
	for _, peerType := range peerTypeLookupOrder() {
		peer, ok := p.cachePeerByIDType(id, peerType)
		if ok {
			return peer
		}
	}
	return &Peer{}
}

func (p *PeerStorage) cachePeerByIDType(id int64, peerType EntityType) (*Peer, bool) {
	var peer Peer
	result := p.SqlSession.Where("id = ? AND type = ?", id, peerType.GetInt()).First(&peer)
	if result.Error != nil || result.RowsAffected == 0 {
		return &Peer{}, false
	}
	key := PeerKey{ID: peer.ID, Type: peer.Type}
	p.peerCache.Set(key, &peer)
	return &peer, true
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
