package shardlite

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func try(err error) {
	if err != nil {
		panic(err)
	}
}

type Storer interface {
	Save(id string, path string) bool
	Load(id string, path string) bool
}

type NullStorage struct {
}

func (s *NullStorage) Save(id string, path string) bool {
	return true
}

func (s *NullStorage) Load(id string, path string) bool {
	return true
}

type LockMaker interface {
	Make(id string) sync.Locker
}

type LocalLockMaker struct {
}

func (s *LocalLockMaker) Make(id string) sync.Locker {
	return &sync.Mutex{}
}

type ShardConfig struct {
	kind         string
	dirPath      string
	ttl          time.Duration
	saveInterval time.Duration
	migrateCb    func(*sql.DB) bool
	storage      Storer
	lockMaker    LockMaker
}

type Shard struct {
	id           string
	active       bool
	db           *sql.DB
	lastUsed     time.Time
	lock         *sync.Mutex
	deactivateCh chan string
	stopCh       chan bool
	lease        sync.Locker
	config       *ShardConfig
}

func newShard(
	id string, deactivateCh chan string, lease sync.Locker,
	config *ShardConfig,
) *Shard {
	return &Shard{
		id:           id,
		active:       false,
		db:           nil,
		lastUsed:     time.Now(),
		lock:         &sync.Mutex{},
		deactivateCh: deactivateCh,
		stopCh:       make(chan bool),
		lease:        lease,
		config:       config,
	}
}

func (s *Shard) path() string {
	return path.Join(
		s.config.dirPath,
		s.config.kind,
		fmt.Sprintf("%s.db", s.id),
	)
}

func (s *Shard) connect() *sql.DB {
	log.Printf("Path %v", s.path())

	db, err := sql.Open("sqlite3", s.path())
	try(err)
	db.Ping()

	return db
}

func (s *Shard) Activate() *sql.DB {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		log.Printf("Activating shard %v", s.id)
		s.config.storage.Load(s.id, s.path())
		s.db = s.connect()
		s.config.migrateCb(s.db)

		go func() {
			for {
				select {
				case <-time.After(s.config.saveInterval):
					if time.Since(s.lastUsed) >= s.config.ttl {
						s.Deactivate()
					} else {
						s.Save()
					}
				case <-time.After(s.config.ttl):
					if time.Since(s.lastUsed) >= s.config.ttl {
						s.Deactivate()
					}
				case <-s.stopCh:
					return
				}
			}
		}()
		s.active = true
	}

	s.lastUsed = time.Now()
	return s.db
}

func (s *Shard) Save() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		return
	}

	log.Printf("Saving shard %v", s.id)

	s.config.storage.Save(s.id, s.path())
}

func (s *Shard) Deactivate() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		return
	}

	log.Printf("Deactivating shard %v", s.id)
	s.config.storage.Save(s.id, s.path())
	err := s.db.Close()
	try(err)

	s.active = false
	s.deactivateCh <- s.id
}

type Silo struct {
	started      bool
	shards       map[string]*Shard
	lock         *sync.Mutex
	stopCh       chan bool
	deactivateCh chan string
	config       *ShardConfig
}

func NewSilo(kind string, dirPath string, migrateCb func(*sql.DB) bool) *Silo {
	return &Silo{
		started:      false,
		shards:       make(map[string]*Shard),
		lock:         &sync.Mutex{},
		stopCh:       make(chan bool, 1),
		deactivateCh: make(chan string),
		config: &ShardConfig{
			kind:         kind,
			dirPath:      dirPath,
			ttl:          time.Duration(5 * time.Second),
			saveInterval: time.Duration(2 * time.Second),
			migrateCb:    migrateCb,
			storage:      &NullStorage{},
			lockMaker:    &LocalLockMaker{},
		},
	}
}

func (s *Silo) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.started {
		return
	}

	err := os.MkdirAll(path.Join(s.config.dirPath, s.config.kind), os.ModePerm)
	try(err)

	go func() {
		for {
			select {
			case shardId := <-s.deactivateCh:
				s.removeShard(shardId)
			case <-s.stopCh:
				return
			}
		}
	}()

	s.started = true
}

func (s *Silo) Stop() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.started {
		return
	}

	s.stopCh <- true
	log.Println("Shutting down")

	wg := &sync.WaitGroup{}

	for _, shard := range s.shards {
		wg.Add(1)
		go func(shard *Shard) {
			shard.lock.Lock()
			defer shard.lock.Unlock()

			shard.Deactivate()
			wg.Done()
		}(shard)
	}

	wg.Wait()
	s.started = false
}

func (s *Silo) Shard(id string) *Shard {
	s.lock.Lock()
	defer s.lock.Unlock()

	shard, ok := s.shards[id]
	if !ok {
		shard = newShard(
			id, s.deactivateCh, s.config.lockMaker.Make(id),
			s.config,
		)
		s.shards[id] = shard
	}

	return shard
}

func (s *Silo) removeShard(id string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.shards, id)
}
