package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
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
	id           string
	dirPath      string
	ttl          time.Duration
	active       bool
	db           *sql.DB
	lastUsed     time.Time
	lock         *sync.Mutex
	deactivateCh chan string
	stopCh       chan bool
	storage      Storer
	lease        sync.Locker
}

func newShard(
	id string, dirPath string, ttl time.Duration,
	deactivateCh chan string, storage Storer, lease sync.Locker,
) *Shard {
	return &Shard{
		id:           id,
		dirPath:      dirPath,
		ttl:          ttl,
		active:       false,
		db:           nil,
		lastUsed:     time.Now(),
		lock:         &sync.Mutex{},
		deactivateCh: deactivateCh,
		stopCh:       make(chan bool),
		storage:      storage,
		lease:        lease,
	}
}

func (s *Shard) path() string {
	return path.Join(
		s.dirPath,
		fmt.Sprintf("%s.db", s.id),
	)
}

func (s *Shard) connect() *sql.DB {
	//log.Printf("Path %v", s.path())

	db, err := sql.Open("sqlite3", s.path())
	try(err)

	// TODO run migrations
	return db
}

func (s *Shard) Activate() *sql.DB {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		log.Printf("Loading shard %v", s.id)
		s.storage.Load(s.id, s.path())
		s.db = s.connect()

		go func() {
			for {
				select {
				case <-time.After(s.ttl):
					unused := time.Since(s.lastUsed)
					if unused >= s.ttl {
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

func (s *Shard) Deactivate() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.active {
		return
	}

	log.Printf("Saving shard %v", s.id)
	s.storage.Save(s.id, s.path())
	err := s.db.Close()
	try(err)

	s.deactivateCh <- s.id
	s.active = false
}

type Silo struct {
	started      bool
	shards       map[string]*Shard
	lock         *sync.Mutex
	shardTtl     time.Duration
	storage      Storer
	lockMaker    LockMaker
	stopCh       chan bool
	deactivateCh chan string
	dirPath      string
}

func NewSilo() *Silo {
	return &Silo{
		started:      false,
		shards:       make(map[string]*Shard),
		lock:         &sync.Mutex{},
		shardTtl:     time.Duration(5 * time.Second),
		storage:      &NullStorage{},
		lockMaker:    &LocalLockMaker{},
		stopCh:       make(chan bool, 1),
		deactivateCh: make(chan string),
		dirPath:      "dbs",
	}
}

func (s *Silo) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.started {
		return
	}

	err := os.MkdirAll(silo.dirPath, os.ModePerm)
	try(err)

	go func() {
		for {
			select {
			case shardId := <-silo.deactivateCh:
				silo.removeShard(shardId)
			case <-silo.stopCh:
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
		shard = newShard(id, s.dirPath, s.shardTtl, s.deactivateCh, s.storage, s.lockMaker.Make(id))
		s.shards[id] = shard
	}

	return shard
}

func (s *Silo) removeShard(id string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.shards, id)
}

var silo = NewSilo()

func handler(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User")
	shard := silo.Shard(userID)
	db := shard.Activate()

	db.Exec("INSERT INTO counters (count) VALUES (1)")

	total := 0
	row := db.QueryRow("SELECT SUM(count) FROM counters")
	row.Scan(&total)

	w.WriteHeader(200)
	fmt.Fprintf(w, "%v\n", total)
}

func main() {
	silo.Start()

	http.HandleFunc("/", handler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
