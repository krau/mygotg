package session

import (
	"context"
	"errors"
	"sync"

	"github.com/gotd/td/session"
	"github.com/krau/mygotg/storage"
)

// SessionStorage keeps a session snapshot backed by peer storage.
type SessionStorage struct {
	data        []byte
	peerStorage *storage.PeerStorage
	mux         sync.Mutex
}

type jsonData struct {
	Version int
	Data    session.Data
}

// LoadSession returns a copy of the current session snapshot.
func (f *SessionStorage) LoadSession(_ context.Context) ([]byte, error) {
	if f == nil {
		return nil, errors.New("nil session storage is invalid")
	}

	f.mux.Lock()
	defer f.mux.Unlock()

	return append([]byte(nil), f.data...), nil
}

// StoreSession copies data into the current snapshot and peer storage.
func (f *SessionStorage) StoreSession(_ context.Context, data []byte) error {
	if f == nil {
		return errors.New("nil session storage is invalid")
	}
	f.mux.Lock()
	defer f.mux.Unlock()

	f.data = append([]byte(nil), data...)
	f.peerStorage.UpdateSession(&storage.Session{
		Version: storage.LatestVersion,
		Data:    f.data,
	})
	return nil
}
