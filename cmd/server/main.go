package main

import (
	"log"

	"proglog/internal/legacy_server"
)

func main() {
	srv := legacy_server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
