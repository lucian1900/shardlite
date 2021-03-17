package shardlite

import (
	"database/sql"
	"fmt"
	"io"
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
	Upload(kind string, id string, file io.ReadSeeker) error
	Download(kind string, id string) (io.Reader, error)
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

func (s *Shard) dbPath() string {
	return path.Join(
		s.config.dirPath,
		s.config.kind,
		fmt.Sprintf("%s.db", s.id),
	)
}

func (s *Shard) connect() *sql.DB {
	log.Printf("Path %v", s.dbPath())

	db, err := sql.Open("sqlite3", s.dbPath())
	try(err)
	try(db.Ping())

	return db
}

func (s *Shard) load() {
	log.Printf("Loading shard %v", s.id)
	download, err := s.config.storage.Download(s.config.kind, s.id)
	if os.IsNotExist(err) {
		return
	} else {
		try(err)
	}
	local, err := os.Create(s.dbPath())
	try(err)
	_, err = io.Copy(local, download)
	try(err)
}

func (s *Shard) save() {
	log.Printf("Saving shard %v", s.id)

	local, err := os.Open(s.dbPath())
	try(err)

	try(s.config.storage.Upload(s.config.kind, s.id, local))
}

func (s *Shard) Activate() *sql.DB {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		log.Printf("Activating shard %v", s.id)
		s.load()
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

	s.save()
}

func (s *Shard) Deactivate() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		return
	}

	log.Printf("Deactivating shard %v", s.id)
	s.save()
	try(s.db.Close())

	s.active = false
	s.deactivateCh <- s.id
}

type Pile struct {
	started      bool
	shards       map[string]*Shard
	lock         *sync.Mutex
	stopCh       chan bool
	deactivateCh chan string
	config       *ShardConfig
}

func NewPile(
	name string, localPath string,
	migrateCb func(*sql.DB) bool, storage Storer,
) *Pile {
	return &Pile{
		started:      false,
		shards:       make(map[string]*Shard),
		lock:         &sync.Mutex{},
		stopCh:       make(chan bool, 1),
		deactivateCh: make(chan string),
		config: &ShardConfig{
			kind:         name,
			dirPath:      localPath,
			ttl:          time.Duration(5 * time.Second),
			saveInterval: time.Duration(2 * time.Second),
			migrateCb:    migrateCb,
			storage:      storage,
			lockMaker:    &LocalLockMaker{},
		},
	}
}

func (p *Pile) Start() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.started {
		return
	}

	err := os.MkdirAll(path.Join(p.config.dirPath, p.config.kind), os.ModePerm)
	try(err)

	go func() {
		for {
			select {
			case shardId := <-p.deactivateCh:
				p.removeShard(shardId)
			case <-p.stopCh:
				return
			}
		}
	}()

	p.started = true
}

func (p *Pile) Stop() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.started {
		return
	}

	p.stopCh <- true
	log.Println("Shutting down")

	wg := &sync.WaitGroup{}

	for _, shard := range p.shards {
		wg.Add(1)
		go func(shard *Shard) {
			shard.lock.Lock()
			defer shard.lock.Unlock()

			shard.Deactivate()
			wg.Done()
		}(shard)
	}

	wg.Wait()
	p.started = false
}

func (p *Pile) Shard(id string) *Shard {
	p.lock.Lock()
	defer p.lock.Unlock()

	shard, ok := p.shards[id]
	if !ok {
		shard = newShard(
			id, p.deactivateCh, p.config.lockMaker.Make(id),
			p.config,
		)
		p.shards[id] = shard
	}

	return shard
}

func (p *Pile) removeShard(id string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.shards, id)
}
