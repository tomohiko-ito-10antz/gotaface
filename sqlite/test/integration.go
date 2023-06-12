package test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Jumpaku/gotaface/dbsql"
)

type Statement struct {
	SQL    string
	Params []any
}

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

func Init(ctx context.Context, execer dbsql.Execer, ddlStmts []Statement, dmlStmts []Statement) error {
	for i, stmt := range ddlStmts {
		_, err := execer.ExecContext(ctx, stmt.SQL, stmt.Params...)
		if err != nil {
			return fmt.Errorf(`fail to execute ddl %d: %w`, i, err)
		}
	}
	for i, stmt := range dmlStmts {
		_, err := execer.ExecContext(ctx, stmt.SQL, stmt.Params...)
		if err != nil {
			return fmt.Errorf(`fail to execute dml %d: %w`, i, err)
		}
	}

	return nil
}

func FindRow[Row any](ctx context.Context, queryer dbsql.Queryer, from string, where map[string]any) (*Row, error) {
	cond := ""
	params := []any{}
	for key, val := range where {
		if cond != "" {
			cond += ` AND `
		}
		cond += key + ` = ?`
		params = append(params, val)
	}
	stmt := fmt.Sprintf(`SELECT * FROM %s WHERE %s`, from, cond)
	rows, err := queryer.QueryContext(ctx, stmt, params...)
	if err != nil {
		return nil, fmt.Errorf(`fail to query row: %w`, err)
	}

	scanned, err := dbsql.ScanRows(rows, dbsql.NewScanRowTypes[Row]())
	if err != nil {
		return nil, fmt.Errorf(`fail to scan row: %w`, err)
	}

	if len(scanned) != 1 {
		return nil, nil
	}

	row := dbsql.StructScanRowValue[Row](scanned[0])

	return &row, nil
}
