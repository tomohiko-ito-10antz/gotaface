package dbschema

import (
	"context"
	"fmt"

	"github.com/Jumpaku/gotaface/ddl/schema"
)

type DBSchemaOutput = schema.SchemaFormat

func FetchSchema(ctx context.Context, fetcher schema.Fetcher) (DBSchemaOutput, error) {
	var output DBSchemaOutput

	s, err := fetcher.Fetch(ctx)
	if err != nil {
		return output, fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	output = schema.SchemaFormat{ReferencesVal: s.References()}
	for _, table := range s.Tables() {
		tableFormat := schema.TableFormat{
			NameVal:       table.Name(),
			PrimaryKeyVal: table.PrimaryKey(),
		}
		for _, column := range table.Columns() {
			tableFormat.ColumnsVal = append(tableFormat.ColumnsVal, schema.ColumnFormat{
				NameVal: column.Name(),
				TypeVal: column.Type(),
			})
		}

		output.TablesVal = append(output.TablesVal, tableFormat)
	}

	return output, nil
}
