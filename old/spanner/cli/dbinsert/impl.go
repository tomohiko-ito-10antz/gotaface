package dbinsert

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml"
	spanner_impl "github.com/Jumpaku/gotaface/spanner"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
	spanner_insert "github.com/Jumpaku/gotaface/spanner/dml/insert"
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
	client, err := spanner.NewClient(ctx, dataSource)
	if err != nil {
		return fmt.Errorf(`fail to create Spanner client %s: %w`, dataSource, err)
	}
	defer client.Close()

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
		schema, err := spanner_schema.FetchSchemaOrUseCache(ctx, schemaReader, schemaWriter, rwt)
		if err != nil {
			return fmt.Errorf(`fail to fetch schema or use cache: %w`, err)
		}

		tableMap := map[string]spanner_schema.Table{}
		for _, table := range schema.TablesVal {
			tableMap[table.Name()] = table
		}

		inserter := spanner_insert.NewInserter(rwt)

		for i := 0; i < input.Len(); i++ {
			input := input.Get(i)
			table := tableMap[input.Name()]
			columnMap := map[string]spanner_schema.Column{}
			for _, column := range table.ColumnsVal {
				columnMap[column.Name()] = column
			}

			rows := dml.Rows{}
			for _, inputRow := range input.Rows() {
				row := dml.Row{}
				for column, value := range inputRow {
					row[column], err = spanner_impl.ToDBValue(columnMap[column].Type(), value)
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

		return nil
	})
	if err != nil {
		return fmt.Errorf(`fail to commit transaction: %w`, err)
	}

	return nil
}
