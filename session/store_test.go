package session_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/krau/mygotg/session"
)

func TestSessionStorageTracksLatestSnapshot(t *testing.T) {
	ctx := context.Background()
	peers, sessions, err := session.NewSessionStorage(ctx, session.SqlSession(sqlite.Open(":memory:")), false)
	if err != nil {
		t.Fatal(err)
	}
	db, err := peers.SqlSession.DB()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	for _, original := range [][]byte{[]byte("synthetic-first-session"), []byte("synthetic-replacement-session"), nil} {
		input := bytes.Clone(original)
		if err := sessions.StoreSession(ctx, input); err != nil {
			t.Fatal(err)
		}
		if len(input) > 0 {
			input[0] ^= 0xff
		}
		loaded, err := sessions.LoadSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(loaded, original) {
			t.Fatalf("loaded snapshot = %q, want %q", loaded, original)
		}
		if persisted := peers.GetSession().Data; !bytes.Equal(persisted, original) {
			t.Fatalf("persisted snapshot = %q, want %q", persisted, original)
		}
		if len(loaded) > 0 {
			loaded[0] ^= 0xff
		}
		again, err := sessions.LoadSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, original) {
			t.Fatalf("caller mutation changed snapshot to %q, want %q", again, original)
		}
	}
}
