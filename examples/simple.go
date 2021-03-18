package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/lucian1900/shardlite"
)

func migrate(db *sql.DB) bool {
	log.Printf("Running migration")

	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS counters (
    	id INTEGER PRIMARY KEY AUTOINCREMENT,
    	count INTEGER
	)`)
	if err != nil {
		panic(err)
	}

	return true
}

type Files struct {
	dirPath string
}

func (f Files) path(kind string, id string) string {
	return path.Join(f.dirPath, kind, fmt.Sprintf("%s.db", id))
}

func (f Files) Upload(kind string, id string, in io.ReadSeeker) error {
	err := os.MkdirAll(path.Join(f.dirPath, kind), os.ModePerm)
	if err != nil {
		return err
	}
	out, err := os.Create(f.path(kind, id))
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return nil
}

func (f Files) Download(kind string, id string) (io.Reader, error) {
	out, err := os.Open(f.path(kind, id))
	if err != nil {
		return nil, err
	}
	return out, nil
}

type LocalLeaser struct {
}

func (s LocalLeaser) Make(id string) sync.Locker {
	return &sync.Mutex{}
}

type API struct {
	users *shardlite.Pile
}

func (a *API) handler(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User")

	shard := a.users.Shard(userID)
	db, err := shard.Activate()
	if err != nil {
		switch e := err.(type) {
		case *shardlite.AlreadyActiveError:
			panic(e)
		default:
			panic(err)
		}
	}

	db.Exec("INSERT INTO counters (count) VALUES (1)")

	total := 0
	row := db.QueryRow("SELECT SUM(count) FROM counters")
	row.Scan(&total)

	w.WriteHeader(200)
	fmt.Fprintf(w, "%v\n", total)
}

func main() {
	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	port := 8080

	config := &shardlite.Config{
		Name:          "users",
		Url:           fmt.Sprintf("http://%s:%d", host, port),
		DbPath:        path.Join(os.TempDir(), "simple"),
		SaveInterval:  time.Duration(2 * time.Second),
		ActivationTtl: time.Duration(5 * time.Second),
		MigrateCb:     migrate,
		Storage:       Files{"dbs"},
		Leaser:        LocalLeaser{},
	}
	api := &API{shardlite.NewPile(config)}
	api.users.Start()

	http.HandleFunc("/", api.handler)

	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		panic(err)
	}
}
