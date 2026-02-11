package storage

import (
	"log"
	"sync"
	"time"

	"github.com/AnimeKaizoku/cacher"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type PeerStorage struct {
	peerCache  *cacher.Cacher[PeerKey, *Peer]
	peerLock   sync.RWMutex
	inMemory   bool
	SqlSession *gorm.DB
}

func NewPeerStorage(dialector gorm.Dialector, inMemory bool) *PeerStorage {
	p := PeerStorage{
		inMemory: inMemory,
	}
	var opts *cacher.NewCacherOpts
	if inMemory {
		opts = nil
	} else {
		opts = &cacher.NewCacherOpts{
			TimeToLive:    6 * time.Hour,
			CleanInterval: 24 * time.Hour,
			Revaluate:     true,
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
	}
	p.peerCache = cacher.NewCacher[PeerKey, *Peer](opts)
	return &p
}
