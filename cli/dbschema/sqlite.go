package dbschema

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	json_schema "github.com/Jumpaku/gotaface/ddl/schema"
	sqlite_schema "github.com/Jumpaku/gotaface/sqlite/ddl/schema"
)

type sqliteRunner struct {
	dataSource string // not nil
}

func (r *sqliteRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	db, err := sql.Open("sqlite", r.dataSource)
	if err != nil {
		return fmt.Errorf(`fail to create sqlite client: %w`, err)
	}

	fetcher := sqlite_schema.NewFetcher(db)

	schema, err := fetcher.Fetch(ctx)
	if err != nil {
		return fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	err = json_schema.WriteSchema(schema, stdout)
	if err != nil {
		return fmt.Errorf(`fail to output schema: %w`, err)
	}

	return nil
}
