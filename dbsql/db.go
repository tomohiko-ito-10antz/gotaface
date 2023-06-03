package dbsql

import (
	"context"
	"database/sql"
)

type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

var _ interface {
	Execer
	Queryer
} = (*sql.DB)(nil)
var _ interface {
	Execer
	Queryer
} = (*sql.Tx)(nil)
var _ interface {
	Execer
	Queryer
} = (*sql.Conn)(nil)
