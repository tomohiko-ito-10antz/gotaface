package dbschema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"

	sqlite_schema "github.com/Jumpaku/gotaface/sqlite/ddl/schema"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteRunner struct {
	DataSource string // not nil
}

func (r *SQLiteRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	db, err := sql.Open("sqlite3", r.DataSource)
	if err != nil {
		return fmt.Errorf(`fail to create sqlite client: %w`, err)
	}

	schema, err := sqlite_schema.NewFetcher(db).Fetch(ctx)
	if err != nil {
		return fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	if err := json.NewEncoder(stdout).Encode(schema); err != nil {
		return fmt.Errorf(`fail to encode table schema as JSON: %w`, err)
	}

	return nil
}
