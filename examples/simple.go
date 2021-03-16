package main

import (
	"fmt"
	"net/http"

	"github.com/lucian1900/shardlite"
)

var silo = shardlite.NewSilo()

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
