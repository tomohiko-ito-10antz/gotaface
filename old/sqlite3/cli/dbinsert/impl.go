package dbinsert

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/Jumpaku/gotaface/old/dml"
	gotaface_sqlite3 "github.com/Jumpaku/gotaface/old/sqlite3"
	sqlite3_schema "github.com/Jumpaku/gotaface/old/sqlite3/ddl/schema"
	sqlite3_insert "github.com/Jumpaku/gotaface/old/sqlite3/dml/insert"
)

type InsertRows = interface {
	Name() string
	Rows() dml.Rows
}
type DBInsertInput = interface {
	Len() int
	Get(i int) InsertRows
}

func DBInsertFunc(ctx context.Context, driver string, dataSource string, schemaReader io.Reader, schemaWriter io.Writer, input DBInsertInput) error {
	db, err := sql.Open(driver, dataSource)
	if err != nil {
		return fmt.Errorf(`fail to open SQLite3 %s: %w`, dataSource, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf(`fail to begin transaction %s: %w`, dataSource, err)
	}
	defer tx.Rollback()

	schema, err := sqlite3_schema.FetchSchemaOrUseCache(ctx, schemaReader, schemaWriter, tx)
	if err != nil {
		return fmt.Errorf(`fail to fetch schema or use cache: %w`, err)
	}

	tableMap := map[string]sqlite3_schema.Table{}
	for _, table := range schema.TablesVal {
		tableMap[table.Name()] = table
	}

	inserter := sqlite3_insert.NewInserter(tx)

	for i := 0; i < input.Len(); i++ {
		input := input.Get(i)
		table := tableMap[input.Name()]
		columnMap := map[string]sqlite3_schema.Column{}
		for _, column := range table.ColumnsVal {
			columnMap[column.Name()] = column
		}

		rows := dml.Rows{}
		for _, inputRow := range input.Rows() {
			row := dml.Row{}
			for column, value := range inputRow {
				row[column], err = gotaface_sqlite3.ToDBValue(columnMap[column].Type(), value)
				if err != nil {
					return fmt.Errorf(`fail to convert value to DB value: %v: %w`, value, err)
				}
			}
			rows = append(rows, row)
		}
		err := inserter.Insert(ctx, input.Name(), rows)
		if err != nil {
			return fmt.Errorf(`fail to insert rows in table %s: %w`, input.Name(), err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(`fail to commit transaction: %w`, err)
	}
	return nil
}
