package main

import (
	"database/sql"
	"fmt"
	"net/http"
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
	Save(path string) bool
	Load(path string) bool
}

type FailStorage struct {
	prefix string
}

func (s FailStorage) Save(path string) bool {
	return false
}

func (s FailStorage) Load(path string) bool {
	return false
}

type Leaser interface {
	sync.Locker
	Refresh()
}

type SimpleLock struct {
	*sync.Mutex
}

func (l SimpleLock) Refresh() {
}

type Shard struct {
	id      string
	storage Storer
	leaser  Leaser
	quitCh  chan bool
}

func NewShard(id string) *Shard {
	return &Shard{
		id:      id,
		quitCh:  make(chan bool),
		storage: FailStorage{},
		leaser:  SimpleLock{},
	}
}

func (s *Shard) Save() bool {
	return false
}

func (s *Shard) path() string {
	return fmt.Sprintf("./dbs/%v.db", s.id)
}

func (s *Shard) GetDB() *sql.DB {
	db, err := sql.Open("sqlite3", s.path())
	try(err)
	return db
}

func (s *Shard) activate() {

	go func() {
		defer s.storage.Save(s.path())
		timer := time.NewTimer(60 * time.Second)

		for {
			select {
			case <-s.quitCh:
				break
			case <-timer.C:
				break
			}
		}
	}()

	s.storage.Load(s.path())
}

func (s *Shard) deactivate() {
	s.quitCh <- true
	s.storage.Save(s.path())
}

type Silo struct {
	shards map[string]*Shard
	lock   *sync.Mutex
}

func NewSilo() *Silo {
	return &Silo{make(map[string]*Shard), &sync.Mutex{}}
}

func (s *Silo) Shard(id string) *Shard {
	s.lock.Lock()
	defer s.lock.Unlock()

	shard, ok := s.shards[id]
	if !ok {
		shard = NewShard(id)
		s.shards[id] = shard
	}
	return shard
}

func (s *Silo) Shutdown() {

}

var silo = NewSilo()

func handler(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User")
	shard := silo.Shard(userID)
	db := shard.GetDB()

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
	silo.Shutdown()
}
