package storage

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AnimeKaizoku/cacher"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type PeerStorage struct {
	peerCache *cacher.Cacher[PeerKey, *Peer]
	peerLock  sync.RWMutex
	// Pending write identities outlive cache eviction until saved or superseded.
	pendingPeers map[PeerKey]*Peer
	inMemory     bool
	SqlSession   *gorm.DB
}

func NewPeerStorage(dialector gorm.Dialector, inMemory bool) *PeerStorage {
	p := PeerStorage{
		inMemory: inMemory,
	}
	var opts *cacher.NewCacherOpts
	if inMemory {
		opts = nil
	} else {
		// Fixed TTL avoids cacher's unsynchronized expiry mutation on reads.
		opts = &cacher.NewCacherOpts{
			TimeToLive:    6 * time.Hour,
			CleanInterval: 24 * time.Hour,
		}
		db, err := gorm.Open(dialector, &gorm.Config{
			SkipDefaultTransaction: true,
			Logger:                 logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			log.Panicln(err)
		}
		p.SqlSession = db
		dB, _ := db.DB()
		dB.SetMaxOpenConns(100)
		if err := p.SqlSession.AutoMigrate(&Session{}, &Peer{}); err != nil {
			log.Panicln(err)
		}
		p.ensurePeerCompositePrimaryKey()
	}
	p.peerCache = cacher.NewCacher[PeerKey, *Peer](opts)
	return &p
}

func (p *PeerStorage) ensurePeerCompositePrimaryKey() {
	if p.inMemory || p.SqlSession == nil {
		return
	}
	if !p.SqlSession.Migrator().HasTable(&Peer{}) {
		return
	}
	needsMigration, err := peerPrimaryKeyNeedsMigration(p.SqlSession)
	if err != nil || !needsMigration {
		return
	}
	switch p.SqlSession.Dialector.Name() {
	case "sqlite":
		if err := migratePeerPrimaryKeySQLite(p.SqlSession); err != nil {
			log.Println("peer primary key migration failed:", err)
		}
	default:
		log.Println("peer primary key migration required for existing DB; please migrate schema manually")
	}
}

func peerPrimaryKeyNeedsMigration(db *gorm.DB) (bool, error) {
	columnTypes, err := db.Migrator().ColumnTypes(&Peer{})
	if err != nil {
		return false, err
	}
	var idPrimaryKey bool
	var typePrimaryKey bool
	for _, columnType := range columnTypes {
		isPrimaryKey, _ := columnType.PrimaryKey()
		switch columnType.Name() {
		case "id":
			idPrimaryKey = isPrimaryKey
		case "type":
			typePrimaryKey = isPrimaryKey
		}
	}
	if !idPrimaryKey {
		return false, nil
	}
	return !typePrimaryKey, nil
}

func migratePeerPrimaryKeySQLite(db *gorm.DB) error {
	peerTable := db.NamingStrategy.TableName("peer")
	newTable := peerTable + "_new"
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", newTable)).Error; err != nil {
			return err
		}
		if err := tx.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER NOT NULL, access_hash INTEGER, type INTEGER NOT NULL, username TEXT, PRIMARY KEY (id, type))", newTable)).Error; err != nil {
			return err
		}
		if err := tx.Exec(fmt.Sprintf("INSERT INTO %s (id, access_hash, type, username) SELECT id, access_hash, type, username FROM %s", newTable, peerTable)).Error; err != nil {
			return err
		}
		if err := tx.Exec(fmt.Sprintf("DROP TABLE %s", peerTable)).Error; err != nil {
			return err
		}
		if err := tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", newTable, peerTable)).Error; err != nil {
			return err
		}
		return nil
	})
}
