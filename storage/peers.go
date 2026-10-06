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

// AddPeersFromDialogs fetches the account dialogs and adds every entity (users,
// chats and channels) to the peer storage. Only usable by user accounts.
func AddPeersFromDialogs(ctx context.Context, raw *tg.Client, peerStorage *PeerStorage) {
	_ = dialogs.NewQueryBuilder(raw).GetDialogs().ForEach(ctx, func(ctx context.Context, e dialogs.Elem) error {
		for cid, channel := range e.Entities.Channels() {
			peerStorage.AddPeer(cid, channel.AccessHash, TypeChannel, channel.Username)
		}
		for uid, user := range e.Entities.Users() {
			peerStorage.AddPeer(uid, user.AccessHash, TypeUser, user.Username)
		}
		for gid := range e.Entities.Chats() {
			peerStorage.AddPeer(gid, DefaultAccessHash, TypeChat, DefaultUsername)
		}
		return nil
	})
}
