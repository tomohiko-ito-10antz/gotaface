package test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/Jumpaku/gotaface/dbsql"
	_ "github.com/mattn/go-sqlite3"
)

type Statement struct {
	SQL    string
	Params []any
}

func Setup(t *testing.T, dbPath string, queryParams string) (interface {
	dbsql.Execer
	dbsql.Queryer
}, func()) {
	t.Helper()

	if dbPath == "" {
		dbPath = ":memory:"
	}

	dataSource := dbPath
	if queryParams != "" {
		dataSource += `?` + queryParams

	}

	db, err := sql.Open("sqlite3", dataSource)
	if err != nil {
		t.Fatalf(`fail to open sqlite3 DB: %v`, err)
	}

	if dbPath == "" {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf(`fail to get sqlite3 connection: %v`, err)
		}
		tearDown := func() {
			conn.Close()
			db.Close()
		}
		return conn, tearDown
	} else {
		tearDown := func() {
			db.Close()
			os.Remove(dbPath)
		}

		return db, tearDown
	}
}

func Init(t *testing.T, execer dbsql.Execer, stmts []Statement) {
	t.Helper()

	for i, stmt := range stmts {
		_, err := execer.ExecContext(context.Background(), stmt.SQL, stmt.Params...)
		if err != nil {
			t.Fatalf(`fail to execute statement %d: %v`, i, err)
		}
	}
}

func FindRow[Row any](t *testing.T, queryer dbsql.Queryer, from string, where map[string]any) *Row {
	t.Helper()

	cond := " TRUE"
	params := []any{}
	for key, val := range where {
		cond += ` AND ` + key + ` = ? `
		params = append(params, val)
	}
	stmt := fmt.Sprintf(`SELECT * FROM %s WHERE %s`, from, cond)
	rows, err := queryer.QueryContext(context.Background(), stmt, params...)
	if err != nil {
		t.Fatalf(`fail to query row: %v`, err)
	}

	scanned, err := dbsql.ScanRows(rows, dbsql.NewScanRowTypes[Row]())
	if err != nil {
		t.Fatalf(`fail to scan row: %v`, err)
	}

	if len(scanned) != 1 {
		return nil
	}

	row := dbsql.StructScanRowValue[Row](scanned[0])

	return &row
}

func ListRows[Row any](t *testing.T, queryer dbsql.Queryer, from string) []*Row {
	t.Helper()

	stmt := fmt.Sprintf(`SELECT * FROM %s`, from)
	rows, err := queryer.QueryContext(context.Background(), stmt)
	if err != nil {
		t.Fatalf(`fail to query row: %v`, err)
	}

	scanned, err := dbsql.ScanRows(rows, dbsql.NewScanRowTypes[Row]())
	if err != nil {
		t.Fatalf(`fail to scan row: %v`, err)
	}

	rowsStruct := []*Row{}
	for _, scanned := range scanned {
		row := dbsql.StructScanRowValue[Row](scanned)
		rowsStruct = append(rowsStruct, &row)
	}

	return rowsStruct
}
