package storage

import (
	"context"
	"database/sql"
	"fmt"
	"message-relay/internal/app/models"
	"strconv"
	"strings"
)

const TABLE = ""

type IRepo interface {
	Get(context.Context, int64, int8) ([]models.Event, error)
	Delete(context.Context, []int64) error
}

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo {
	return &Repo{
		db: db,
	}
}

func (r *Repo) Get(ctx context.Context, lastID int64, maxSize int8) ([]models.Event, error) {
	var events []models.Event

	query := fmt.Sprintf(`SELECT * FROM %s WHERE _id > %d LIMIT %d`, TABLE, lastID, maxSize)
	rows, err := r.db.QueryContext(ctx, query)
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

func (r *Repo) Delete(ctx context.Context, listID []int64) error {
	listIDString := make([]string, len(listID))
	for index, _ := range listID {
		id := listID[index]
		idString := strconv.FormatInt(id, 10)
		listIDString = append(listIDString, idString)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE _id IN (%s)", TABLE, strings.Join(listIDString, ", "))
	_, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}
