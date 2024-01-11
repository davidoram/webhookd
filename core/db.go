package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/davidoram/webhookd/view"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
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
	data, err := json.Marshal(sub)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO subscriptions (id, data, created_at, updated_at, deleted_at) VALUES (?,?,?,?,?)
	`, sub.ID, data, sub.CreatedAt, sub.UpdatedAt, sub.DeletedAt)
	return err
}

// GetSubscriptionsUpdatedSince returns a list of subscriptions that have been updated since the specified time
// it includes soft deleted subscriptions
func GetSubscriptionsUpdatedSince(ctx context.Context, db *sql.DB, since time.Time, offset, limit int64) (view.SubscriptionCollection, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, data, created_at, updated_at, deleted_at 
		FROM subscriptions 
		WHERE updated_at > ?
		ORDER BY created_at
		LIMIT ?
		OFFSET ?`, since, limit, offset)
	if err != nil {
		return view.SubscriptionCollection{}, err
	}
	defer rows.Close()

	return mapRows(rows, offset, limit)
}

// GetActiveSubscriptions returns a list of all subscriptions, excluding soft deleted subscriptions
func GetActiveSubscriptions(ctx context.Context, db *sql.DB, offset, limit int64) (view.SubscriptionCollection, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, data, created_at, updated_at, deleted_at 
		FROM subscriptions 
		WHERE deleted_at IS NULL
		ORDER BY created_at
		LIMIT ?
		OFFSET ?`, limit, offset)
	if err != nil {
		return view.SubscriptionCollection{}, err
	}
	defer rows.Close()

	return mapRows(rows, offset, limit)
}

// GetSubscriptions returns a list of all subscriptions
func GetSubscriptions(ctx context.Context, db *sql.DB, offset, limit int64) (view.SubscriptionCollection, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, data, created_at, updated_at, deleted_at 
		FROM subscriptions 
		ORDER BY created_at
		LIMIT ?
		OFFSET ?`, limit, offset)
	if err != nil {
		return view.SubscriptionCollection{}, err
	}
	defer rows.Close()

	return mapRows(rows, offset, limit)
}

func mapRows(rows *sql.Rows, offset, limit int64) (view.SubscriptionCollection, error) {
	subs := view.SubscriptionCollection{Subscriptions: []view.Subscription{}, Offset: offset, Limit: limit}
	for rows.Next() {
		id := uuid.UUID{}
		data := []byte{}
		createdAt := time.Time{}
		updatedAt := time.Time{}
		deletedAt := sql.NullTime{}
		err := rows.Scan(&id, &data, &createdAt, &updatedAt, &deletedAt)
		if err != nil {
			return subs, err
		}
		sub, err := view.NewSubscriptionFromJSON(id, data, createdAt, updatedAt, deletedAt)
		if err != nil {
			return subs, err
		}
		subs.Subscriptions = append(subs.Subscriptions, sub)
	}
	return subs, nil
}
