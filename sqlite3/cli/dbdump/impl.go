package dbdump

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Jumpaku/gotaface/dml"
	sqlite3_schema "github.com/Jumpaku/gotaface/sqlite3/ddl/schema"
	sqlite3_dump "github.com/Jumpaku/gotaface/sqlite3/dml/dump"
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

	var schema *sqlite3_schema.Schema
	if schemaReader == nil {
		var err error
		schema, err = sqlite3_schema.FetchSchema(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf(`fail to fetch schema: %w`, err)
		}

		if err := json.NewEncoder(schemaWriter).Encode(schema); err != nil {
			return nil, fmt.Errorf(`fail to encode schema JSON: %w`, err)
		}
	} else {
		schema = new(sqlite3_schema.Schema)
		if err := json.NewDecoder(schemaReader).Decode(schema); err != nil {
			return nil, fmt.Errorf(`fail to decode schema JSON: %w`, err)
		}
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
