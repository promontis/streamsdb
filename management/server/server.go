package main

import (
	"context"
	log "log"
	http "net/http"
	os "os"

	"github.com/99designs/gqlgen/handler"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pjvds/streamsdb/management"
	"github.com/pjvds/streamsdb/storage"
	"go.uber.org/zap"
)

const defaultPort = "8080"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	logger, _ := zap.NewDevelopment()
	store := storage.OpenFdb(db, logger)
	go store.RunTailers(context.TODO())

	http.Handle("/", handler.Playground("GraphQL playground", "/query"))
	http.Handle("/query", handler.GraphQL(management.NewExecutableSchema(management.Config{Resolvers: &management.Resolver{store}})))

	log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
