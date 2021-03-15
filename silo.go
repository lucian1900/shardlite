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
	id       string
	storage  Storer
	lock     *sync.Mutex
	lease    sync.Locker
	lastUsed time.Time
	dirPath  string
	db       *sql.DB
}

func newShard(id string, dirPath string, storage Storer, lease sync.Locker) *Shard {
	return &Shard{
		id:       id,
		storage:  storage,
		lock:     &sync.Mutex{},
		lease:    lease,
		lastUsed: time.Now(),
		dirPath:  dirPath,
		db:       nil,
	}
}

func (s *Shard) path() string {
	return path.Join(
		s.dirPath,
		fmt.Sprintf("%s.db", s.id),
	)
}

func (s *Shard) connect() *sql.DB {
	log.Printf("Path %v", s.path())

	db, err := sql.Open("sqlite3", s.path())
	try(err)
	return db
}

func (s *Shard) DB() *sql.DB {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.lastUsed = time.Now()

	if s.db == nil {
		log.Println("Loading shard")
		s.storage.Load(s.id, s.path())
		s.db = s.connect()
	}

	return s.db
}

func (s *Shard) save() {
	s.storage.Save(s.id, s.path())
}

func (s *Shard) Close() {
	log.Printf("Closing shard %v", s.id)
	s.save()
	s.db = nil
}

type Silo struct {
	shards    map[string]*Shard
	lock      *sync.Mutex
	shardTtl  time.Duration
	storage   Storer
	lockMaker LockMaker
	quitCh    chan bool
	dirPath   string
}

func NewSilo() *Silo {
	silo := &Silo{
		shards:    make(map[string]*Shard),
		lock:      &sync.Mutex{},
		shardTtl:  time.Duration(15 * time.Second),
		storage:   &NullStorage{},
		lockMaker: &LocalLockMaker{},
		quitCh:    make(chan bool, 1),
		dirPath:   "./dbs",
	}

	err := os.MkdirAll(silo.dirPath, os.ModePerm)
	try(err)

	// Expire in the background
	go func() {
		ticker := time.Tick(5 * time.Second)

		for {
			select {
			case <-ticker:
				silo.expire()
			case <-silo.quitCh:
				return
			}
		}
	}()

	return silo
}

func (s *Silo) Shard(id string) *Shard {
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Println("Finding shard")

	shard, ok := s.shards[id]
	if !ok {
		shard = newShard(id, s.dirPath, s.storage, s.lockMaker.Make(id))
		s.shards[id] = shard
	}

	return shard
}

func (s *Silo) Shutdown() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.quitCh <- true
	log.Println("Shutting down")

	wg := &sync.WaitGroup{}

	for _, shard := range s.shards {
		wg.Add(1)
		go func(shard *Shard) {
			shard.lock.Lock()
			defer shard.lock.Unlock()

			shard.Close()
			wg.Done()
		}(shard)
	}

	wg.Wait()
}

func (s *Silo) expire() {
	s.lock.Lock()
	defer s.lock.Unlock()

	cutoff := time.Now().Add(-s.shardTtl)

	for _, shard := range s.shards {
		if shard.lastUsed.After(cutoff) {
			continue
		}

		// Save in parallel, without holding the silo lock
		go func(shard *Shard) {
			shard.lock.Lock()
			defer shard.lock.Unlock()

			shard.Close()

			s.lock.Lock()
			defer s.lock.Unlock()
			delete(s.shards, shard.id)
		}(shard)
	}
}

var silo = NewSilo()

func handler(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User")
	shard := silo.Shard(userID)
	db := shard.DB()

	db.Exec("INSERT INTO counters (count) VALUES (1)")

	total := 0
	row := db.QueryRow("SELECT SUM(count) FROM counters")
	row.Scan(&total)

	w.WriteHeader(200)
	fmt.Fprintf(w, "%v\n", total)
}

func main() {
	http.HandleFunc("/", handler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
