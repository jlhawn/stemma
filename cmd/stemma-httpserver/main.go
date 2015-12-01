package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/jlhawn/stemma"
)

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage: stemma-httpserver PORT")
		os.Exit(1)
	}

	repo, err := stemma.NewRepository(".")
	if err != nil {
		log.Fatalf("unable to initialize repository: %s", err)
	}

	r := mux.NewRouter()
	r.Queries("service", "get-tag").HandlerFunc(repo.HandleGetTag)
	r.Queries("service", "list-tags").HandlerFunc(repo.HandleListTags)
	r.Queries("service", "serve-objects").HandlerFunc(repo.HandleServeObjects)
	r.Queries("service", "receive-objects").HandlerFunc(repo.HandleReceiveObjects)

	log.Fatal(http.ListenAndServe(flag.Arg(0), r))
}
