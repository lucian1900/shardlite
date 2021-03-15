package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"sort"
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
	db       *sql.DB
}

func newShard(id string, storage Storer, lock sync.Locker) *Shard {
	return &Shard{
		id:       id,
		storage:  storage,
		lock:     &sync.Mutex{},
		lease:    lock,
		lastUsed: time.Now(),
		db:       nil,
	}
}

func (s *Shard) path() string {
	return fmt.Sprintf("./dbs/%v.db", s.id)
}

func (s *Shard) connect() *sql.DB {
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

type LRURecord struct {
	id string
	ts time.Time
}

type Silo struct {
	shards    map[string]*Shard
	lock      *sync.Mutex
	lru       []LRURecord
	storage   Storer
	lockMaker LockMaker
	quitCh    chan bool
}

func NewSilo(capacity int) *Silo {
	silo := &Silo{
		shards:    make(map[string]*Shard, capacity),
		lock:      &sync.Mutex{},
		lru:       make([]LRURecord, 0, capacity),
		storage:   &NullStorage{},
		lockMaker: &LocalLockMaker{},
		quitCh:    make(chan bool, 1),
	}

	go func() {
		ticker := time.Tick(5 * time.Second)

		for {
			select {
			case <-ticker:
				silo.expire(15 * time.Second)
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
		shard = newShard(id, s.storage, s.lockMaker.Make(id))
		s.shards[id] = shard
	}

	s.lru = append(s.lru, LRURecord{id, time.Now()})

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

func (s *Silo) expire(ttl time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()

	log.Printf("Expiring by lru len %v cap %v %v", len(s.lru), cap(s.lru), s.lru)

	cutoff := time.Now().Add(-ttl)
	index := sort.Search(len(s.lru), func(i int) bool {
		return s.lru[i].ts.After(cutoff)
	})

	log.Printf("Cutoff %v", index)

	if index == 0 {
		log.Printf("Nothing to expire")
		return
	}

	shardsSeen := make(map[string]bool)

	for _, rec := range s.lru[:index] {
		shard, found := s.shards[rec.id]
		if !found || shard.lastUsed.After(cutoff) {
			continue
		}
		_, seen := shardsSeen[rec.id]
		if seen {
			continue
		} else {
			shardsSeen[rec.id] = true
		}

		// Save in parallel, without holding the silo lock
		go func(rec LRURecord) {
			shard.lock.Lock()
			defer shard.lock.Unlock()

			shard.Close()

			s.lock.Lock()
			defer s.lock.Unlock()
			delete(s.shards, rec.id)
		}(rec)
	}

	s.lru = s.lru[index:]
}

var silo = NewSilo(1)

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
