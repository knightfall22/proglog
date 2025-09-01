package main

import (
	"log"

	"github.com/knightfall22/proglog/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
