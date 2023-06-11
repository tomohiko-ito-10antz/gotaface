package test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Jumpaku/gotaface/dbsql"
)

func Setup() (interface {
	dbsql.Execer
	dbsql.Queryer
}, func(), error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, nil, fmt.Errorf(`fail to open sqlite DB: %w`, err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf(`fail to open sqlite DB: %w`, err)
	}

	tearDown := func() {
		db.Close()
	}

	return conn, tearDown, nil
}
