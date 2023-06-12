package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/Jumpaku/gotaface/dbsql"
)

type Statement struct {
	SQL    string
	Params []any
}

func Setup(t *testing.T) (interface {
	dbsql.Execer
	dbsql.Queryer
}, func()) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf(`fail to open sqlite DB: %v`, err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf(`fail to get sqlite connection: %v`, err)
	}

	tearDown := func() {
		db.Close()
	}

	return conn, tearDown
}

func Init(t *testing.T, execer dbsql.Execer, stmts []Statement) {
	t.Helper()

	for i, stmt := range stmts {
		_, err := execer.ExecContext(context.Background(), stmt.SQL, stmt.Params...)
		if err != nil {
			t.Fatalf(`fail to execute ddl %d: %v`, i, err)
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
