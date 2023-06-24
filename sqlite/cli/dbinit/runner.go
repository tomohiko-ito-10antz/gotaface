package dbinit

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/Jumpaku/gotaface/cli/dbinit"
	"github.com/Jumpaku/gotaface/ddl/schema"
	sqlite_schema "github.com/Jumpaku/gotaface/sqlite/ddl/schema"
	sqlite_delete "github.com/Jumpaku/gotaface/sqlite/dml/delete"
	sqlite_insert "github.com/Jumpaku/gotaface/sqlite/dml/insert"
)

type SqliteRunner struct {
	DataSource string         // not nil
	Fetcher    schema.Fetcher // if nil default fetcher in sqlite_schema is used
}

func (r *SqliteRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	targets, err := dbinit.LoadDBInitInput(stdin)
	if err != nil {
		return fmt.Errorf(`fail to load table initialization data from stdin: %w`, err)
	}

	db, err := sql.Open("sqlite", r.DataSource)
	if err != nil {
		return fmt.Errorf(`fail to create sqlite client: %w`, err)
	}
	var fetcher = r.Fetcher
	if fetcher == nil {
		fetcher = sqlite_schema.NewFetcher(db)
	}
	schema, err := fetcher.Fetch(ctx)
	if err != nil {
		return fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	deleteTables, insertTableRows, err := dbinit.PrepareTableRows(schema, targets)
	if err != nil {
		return fmt.Errorf(`fail to prepare tables: %w`, err)
	}

	err = dbinit.DeleteRowsInParallel(ctx, sqlite_delete.NewDeleter(db), deleteTables)
	if err != nil {
		return fmt.Errorf(`fail to delete rows in tables: %w`, err)
	}

	err = dbinit.InsertRowsInParallel(ctx, sqlite_insert.NewInserter(db), insertTableRows)
	if err != nil {
		return fmt.Errorf(`fail to insert rows in tables: %w`, err)
	}

	return nil
}
