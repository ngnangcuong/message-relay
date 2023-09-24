package storage

import (
	"context"
	"database/sql"
	"fmt"
	"message-relay/internal/app/models"
	"time"
)

type IRepo interface {
	WithTx(*sql.Tx) IRepo
	Get(context.Context, time.Duration, int8) ([]models.Event, error)
	Delete(context.Context, time.Duration) error
	GetAEventForUpdate(context.Context, time.Duration, int64) (models.Event, error)
	UpdateProcessed(context.Context, time.Duration, int64) error
}

type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

type Repo struct {
	db    DBTX
	table string
}

func NewRepo(db DBTX, table string) IRepo {
	return &Repo{
		db:    db,
		table: table,
	}
}

func (r *Repo) WithTx(tx *sql.Tx) IRepo {
	return &Repo{
		db: tx,
	}
}

func (r *Repo) Get(ctx context.Context, timeout time.Duration, maxSize int8) ([]models.Event, error) {
	var events []models.Event

	query := fmt.Sprintf(`SELECT * FROM $1 WHERE is_processed = false order by _id limit $2 FOR UPDATE SKIP LOCKED`)
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rows, err := r.db.QueryContext(c, query, r.table, maxSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var event models.Event
		if err := rows.Scan(&event.ID); err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (r *Repo) Delete(ctx context.Context, timeout time.Duration) error {
	query := fmt.Sprintf("DELETE FROM $1 WHERE is_processed = true")
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := r.db.ExecContext(c, query, r.table)
	if err != nil {
		return err
	}

	return nil
}

func (r *Repo) GetAEventForUpdate(ctx context.Context, timeout time.Duration, id int64) (models.Event, error) {
	var event models.Event
	query := fmt.Sprintf("SELECT * FROM $1 WHERE is_processed = false and _id = $2 FOR UPDATE SKIP LOCKED")
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	row := r.db.QueryRowContext(c, query, r.table, id)
	err := row.Scan(&event.ID)
	return event, err
}

func (r *Repo) UpdateProcessed(ctx context.Context, timeout time.Duration, id int64) error {
	query := fmt.Sprintf("UPDATE $1 SET is_processed = true WHERE _id = $2")
	c, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := r.db.ExecContext(c, query, r.table, id)
	return err
}
