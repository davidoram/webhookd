package core

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestMigrateDB(t *testing.T) {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)
	defer db.Close()
	assert.NoError(t, MigrateDB(context.Background(), db))
}
