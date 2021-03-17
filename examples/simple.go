package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

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
	api := &API{shardlite.NewSilo("users", "dbs", migrate)}
	api.users.Start()

	http.HandleFunc("/", api.handler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
