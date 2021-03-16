package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"github.com/lucian1900/shardlite"
)

type API struct {
	users *shardlite.Silo
}

func migrate(db *sql.DB) bool {
	log.Printf("Running migration")

	_, err := db.Exec(`create table if not exists counters (
    	id integer primary key autoincrement,
    	count integer
	)`)
	if err != nil {
		panic(err)
	}

	return true
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
	api := &API{shardlite.NewSilo("user", "dbs", migrate)}
	api.users.Start()

	http.HandleFunc("/", api.handler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
