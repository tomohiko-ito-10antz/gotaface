package dbinit

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/Jumpaku/gotaface/ddl/schema"
	sqlite_schema "github.com/Jumpaku/gotaface/sqlite/ddl/schema"
	sqlite_delete "github.com/Jumpaku/gotaface/sqlite/dml/delete"
	sqlite_insert "github.com/Jumpaku/gotaface/sqlite/dml/insert"
)

type sqliteRunner struct {
	dataSource string         // not nil
	fetcher    schema.Fetcher // if nil default fetcher in sqlite_schema is used
}

func (r *sqliteRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	targets, err := LoadDBInitInput(stdin)
	if err != nil {
		return fmt.Errorf(`fail to load table initialization data from stdin: %w`, err)
	}

	db, err := sql.Open("sqlite", r.dataSource)
	if err != nil {
		return fmt.Errorf(`fail to create sqlite client: %w`, err)
	}
	var fetcher = r.fetcher
	if fetcher == nil {
		fetcher = sqlite_schema.NewFetcher(db)
	}
	schema, err := fetcher.Fetch(ctx)
	if err != nil {
		return fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	deleteTables, insertTableRows, err := PrepareTableRows(ctx, schema, targets)
	if err != nil {
		return fmt.Errorf(`fail to prepare tables: %w`, err)
	}

	err = DeleteRowsInParallel(ctx, sqlite_delete.NewDeleter(db), deleteTables)
	if err != nil {
		return fmt.Errorf(`fail to delete rows in tables: %w`, err)
	}

	err = InsertRowsInParallel(ctx, sqlite_insert.NewInserter(db), insertTableRows)
	if err != nil {
		return fmt.Errorf(`fail to insert rows in tables: %w`, err)
	}

	return nil
}
