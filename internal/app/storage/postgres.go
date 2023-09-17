package storage

import (
	"database/sql"
	"time"

	"message-relay/internal/app/config"

	_ "github.com/lib/pq"
)

var db *sql.DB

func initPostgre(cfg *config.Config) (*sql.DB, error) {
	d, err := sql.Open("postgres", cfg.Database.Url)
	if err != nil {
		return nil, err
	}

	if err := d.Ping(); err != nil {
		return nil, err
	}

	d.SetConnMaxLifetime(time.Hour)
	d.SetMaxIdleConns(10)
	d.SetMaxOpenConns(200)

	return d, nil
}

func GetPostgres(cfg *config.Config) (*sql.DB, error) {
	if db == nil {
		return initPostgre(cfg)
	}

	return db, nil
}
