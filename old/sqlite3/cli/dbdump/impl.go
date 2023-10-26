package dbdump

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/Jumpaku/gotaface/old/dml"
	sqlite3_schema "github.com/Jumpaku/gotaface/old/sqlite3/ddl/schema"
	sqlite3_dump "github.com/Jumpaku/gotaface/old/sqlite3/dml/dump"
)

type DBDumpInput = []string
type DBDumpOutput = map[string]dml.Rows

func DBDumpFunc(ctx context.Context, driver string, dataSource string, schemaReader io.Reader, schemaWriter io.Writer, input DBDumpInput) (DBDumpOutput, error) {
	db, err := sql.Open(driver, dataSource)
	if err != nil {
		return nil, fmt.Errorf(`fail to open SQLite3 %s: %w`, dataSource, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf(`fail to begin transaction %s: %w`, dataSource, err)
	}
	defer tx.Rollback()

	schema, err := sqlite3_schema.FetchSchemaOrUseCache(ctx, schemaReader, schemaWriter, tx)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch schema or use cache: %w`, err)
	}

	dumper := sqlite3_dump.NewDumper(tx, schema)

	output := DBDumpOutput{}
	for _, target := range input {
		rows, err := dumper.Dump(ctx, target)
		if err != nil {
			return nil, fmt.Errorf(`fail to dump rows in table %s: %w`, target, err)
		}

		output[target] = rows
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf(`fail to commit transaction: %w`, err)
	}
	return output, nil
}
