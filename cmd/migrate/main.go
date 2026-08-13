package main

import (
	"database/sql"
	"flag"
	"log"
	"os"

	"github.com/1hoodlab/go-indexing.git/internal/config"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func main() {
	dir := flag.String("dir", "migrations", "migrations directory")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("usage: go run ./cmd/migrate -- up|down|status")
	}

	cfg := config.Load()
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := goose.SetDialect("postgres"); err != nil {
		log.Fatalf("dialect: %v", err)
	}
	if err := goose.Run(args[0], db, *dir, args[1:]...); err != nil {
		log.Fatalf("goose %s: %v", args[0], err)
	}
	os.Exit(0)
}
