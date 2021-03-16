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

type Shard struct {
	shardType    string
	id           string
	dirPath      string
	ttl          time.Duration
	saveInterval time.Duration
	active       bool
	db           *sql.DB
	lastUsed     time.Time
	lock         *sync.Mutex
	migrateCb    func(*sql.DB) bool
	deactivateCh chan string
	stopCh       chan bool
	storage      Storer
	lease        sync.Locker
}

func newShard(
	shardType string, id string, dirPath string,
	ttl time.Duration, saveInterval time.Duration,
	migrateCb func(*sql.DB) bool, deactivateCh chan string,
	storage Storer, lease sync.Locker,
) *Shard {
	return &Shard{
		shardType:    shardType,
		id:           id,
		dirPath:      dirPath,
		ttl:          ttl,
		saveInterval: saveInterval,
		active:       false,
		db:           nil,
		lastUsed:     time.Now(),
		lock:         &sync.Mutex{},
		migrateCb:    migrateCb,
		deactivateCh: deactivateCh,
		stopCh:       make(chan bool),
		storage:      storage,
		lease:        lease,
	}
}

func (s *Shard) path() string {
	return path.Join(
		s.dirPath,
		s.shardType,
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
		s.storage.Load(s.id, s.path())
		s.db = s.connect()
		s.migrateCb(s.db)

		go func() {
			for {
				select {
				case <-time.After(s.saveInterval):
					if time.Since(s.lastUsed) >= s.ttl {
						s.Deactivate()
					} else {
						s.Save()
					}
				case <-time.After(s.ttl):
					if time.Since(s.lastUsed) >= s.ttl {
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

	s.storage.Save(s.id, s.path())
}

func (s *Shard) Deactivate() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		return
	}

	log.Printf("Deactivating shard %v", s.id)
	s.storage.Save(s.id, s.path())
	err := s.db.Close()
	try(err)

	s.active = false
	s.deactivateCh <- s.id
}

type Silo struct {
	shardType         string
	started           bool
	shards            map[string]*Shard
	lock              *sync.Mutex
	shardTtl          time.Duration
	shardSaveInterval time.Duration
	storage           Storer
	lockMaker         LockMaker
	migrateCb         func(*sql.DB) bool
	stopCh            chan bool
	deactivateCh      chan string
	dirPath           string
}

func NewSilo(shardType string, dirPath string, migrateCb func(*sql.DB) bool) *Silo {
	return &Silo{
		shardType:         shardType,
		started:           false,
		shards:            make(map[string]*Shard),
		lock:              &sync.Mutex{},
		shardTtl:          time.Duration(5 * time.Second),
		shardSaveInterval: time.Duration(2 * time.Second),
		storage:           &NullStorage{},
		lockMaker:         &LocalLockMaker{},
		migrateCb:         migrateCb,
		stopCh:            make(chan bool, 1),
		deactivateCh:      make(chan string),
		dirPath:           dirPath,
	}
}

func (s *Silo) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.started {
		return
	}

	err := os.MkdirAll(path.Join(s.dirPath, s.shardType), os.ModePerm)
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
			s.shardType, id, s.dirPath, s.shardTtl, s.shardSaveInterval,
			s.migrateCb, s.deactivateCh,
			s.storage, s.lockMaker.Make(id),
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
