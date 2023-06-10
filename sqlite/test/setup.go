package test

import (
	"database/sql"
	"fmt"
)

func Setup() (*sql.DB, func(), error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, nil, fmt.Errorf(`fail to open sqlite DB: %w`, err)
	}

	tearDown := func() {
		db.Close()
	}

	return db, tearDown, nil
}
