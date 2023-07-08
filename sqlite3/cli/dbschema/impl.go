package dbschema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/Jumpaku/gotaface/ddl/schema"
	sqlite3_schema "github.com/Jumpaku/gotaface/sqlite3/ddl/schema"
	_ "github.com/mattn/go-sqlite3"
)

type DBSchemaOutput = interface {
	json.Marshaler
	schema.Schema
}

func DBSchemaFunc(ctx context.Context, driver string, dataSource string) (DBSchemaOutput, error) {
	db, err := sql.Open("sqlite3", dataSource)
	if err != nil {
		return nil, fmt.Errorf(`fail to create sqlite3 client: %w`, err)
	}
	defer db.Close()

	schema, err := sqlite3_schema.NewFetcher(db).Fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	return schema.(*sqlite3_schema.Schema), nil
}
