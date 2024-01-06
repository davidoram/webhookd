package core

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
)

func MigrateDB(ctx context.Context, db *sql.DB) error {

	// Create the subscriptions table if not exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS subscriptions (
			id TEXT PRIMARY KEY,  
			data TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			deleted_at DATETIME
		)
	`)
	return err
}

func InsertSubscription(ctx context.Context, db *sql.DB, sub Subscription) error {
	data, err := sub.MarshallForDatabase()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO subscriptions (id, data, created_at, updated_at, deleted_at) VALUES (?, ?)
	`, sub.ID, data, sub.CreatedAt, sub.UpdatedAt, sub.DeletedAt)
	return err
}

// GetSubscriptionsUpdatedSince returns a list of subscriptions that have been updated since the specified time
// it includes soft deleted subscriptions
func GetSubscriptionsUpdatedSince(ctx context.Context, db *sql.DB, since time.Time) ([]Subscription, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, data, created_at, updated_at FROM subscriptions WHERE updated_at > ?
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return mapRows(rows)
}

// GetActiveSubscriptions returns a list of all subscriptions, excluding soft deleted subscriptions
func GetActiveSubscriptions(ctx context.Context, db *sql.DB) ([]Subscription, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, data, created_at, updated_at FROM subscriptions WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return mapRows(rows)
}

func mapRows(rows *sql.Rows) ([]Subscription, error) {
	subs := []Subscription{}
	for rows.Next() {
		id := uuid.UUID{}
		data := []byte{}
		createdAt := time.Time{}
		updatedAt := time.Time{}
		err := rows.Scan(&id, &data, &createdAt, &updatedAt)
		if err != nil {
			return nil, err
		}
		sub, err := NewSubscriptionFromJSON(id, data, createdAt, updatedAt)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}
