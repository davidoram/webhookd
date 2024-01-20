package core

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

// OpenTestDatabase opens an in-memory SQLite database for testing, and runs all the migrations
func OpenTestDatabase(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	// Migrate the db
	assert.NoError(t, MigrateDB(context.Background(), db))
	return db
}
