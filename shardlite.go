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

type LockerMaker interface {
	Make(id string) sync.Locker
}

type ActivationError struct {
	Host string
	Port int16
}

func (e *ActivationError) Error() string {
	return fmt.Sprintf("already active: %s:%d", e.Host, e.Port)
}

type Config struct {
	Name          string
	DbPath        string
	MigrateCb     func(*sql.DB) bool
	ActivationTtl time.Duration
	SaveInterval  time.Duration
	Storage       Storer
	Leaser        LockerMaker
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
	config       *Config
}

func newShard(
	id string, deactivateCh chan string, lease sync.Locker,
	config *Config,
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
		s.config.DbPath,
		s.config.Name,
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
	download, err := s.config.Storage.Download(s.config.Name, s.id)
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

	db := s.connect()
	db.Exec("BEGIN EXCLUSIVE TRANSACTION")
	defer db.Exec("END TRANSACTION")

	local, err := os.Open(s.dbPath())
	try(err)

	try(s.config.Storage.Upload(s.config.Name, s.id, local))
}

func (s *Shard) Activate() (*sql.DB, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		log.Printf("Activating shard %v", s.id)
		s.load()
		s.db = s.connect()
		s.config.MigrateCb(s.db)

		go func() {
			for {
				select {
				case <-time.After(s.config.SaveInterval):
					if time.Since(s.lastUsed) >= s.config.ActivationTtl {
						s.Deactivate()
					} else {
						s.Save()
					}
				case <-time.After(s.config.ActivationTtl):
					if time.Since(s.lastUsed) >= s.config.ActivationTtl {
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
	return s.db, nil
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
	config       *Config
}

func NewPile(config *Config) *Pile {
	return &Pile{
		started:      false,
		shards:       make(map[string]*Shard),
		lock:         &sync.Mutex{},
		stopCh:       make(chan bool, 1),
		deactivateCh: make(chan string),
		config:       config,
	}
}

func (p *Pile) Start() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.started {
		return
	}

	err := os.MkdirAll(path.Join(p.config.DbPath, p.config.Name), os.ModePerm)
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
			id, p.deactivateCh, p.config.Leaser.Make(id),
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
