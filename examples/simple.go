package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"

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

type API struct {
	users *shardlite.Silo
}

func (a *API) handler(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User")
	shard := a.users.Shard(userID)
	db := shard.Activate()

	db.Exec("INSERT INTO counters (count) VALUES (1)")

	total := 0
	row := db.QueryRow("SELECT SUM(count) FROM counters")
	row.Scan(&total)

	w.WriteHeader(200)
	fmt.Fprintf(w, "%v\n", total)
}

func main() {
	files := Files{"dbs"}

	api := &API{shardlite.NewSilo(
		"users",
		path.Join(os.TempDir(), "simple"),
		migrate,
		files,
	)}
	api.users.Start()

	http.HandleFunc("/", api.handler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
