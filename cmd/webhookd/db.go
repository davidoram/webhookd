package main

import (
	"context"
	"database/sql"
)

func MigrateDB(ctx context.Context, db *sql.DB) error {

	// Create the subscriptions table if not exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS subscriptions (
			id TEXT PRIMARY KEY,  
			data TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	return err
}
