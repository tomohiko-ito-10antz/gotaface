package dbdump

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/old/dml"
	spanner_schema "github.com/Jumpaku/gotaface/old/spanner/ddl/schema"
	spanner_dump "github.com/Jumpaku/gotaface/old/spanner/dml/dump"
)

type DBDumpInput = []string
type DBDumpOutput = map[string]dml.Rows

func DBDumpFunc(ctx context.Context, driver string, dataSource string, schemaReader io.Reader, schemaWriter io.Writer, input DBDumpInput) (DBDumpOutput, error) {
	client, err := spanner.NewClient(ctx, dataSource)
	if err != nil {
		return nil, fmt.Errorf(`fail to create Spanner client %s: %w`, dataSource, err)
	}
	defer client.Close()

	rtx := client.ReadOnlyTransaction()
	defer rtx.Close()

	schema, err := spanner_schema.FetchSchemaOrUseCache(ctx, schemaReader, schemaWriter, rtx)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch schema or use cache: %w`, err)
	}

	dumper := spanner_dump.NewDumper(rtx, schema)

	output := DBDumpOutput{}
	for _, target := range input {
		rows, err := dumper.Dump(ctx, target)
		if err != nil {
			return nil, fmt.Errorf(`fail to dump rows in table %s: %w`, target, err)
		}

		output[target] = rows
	}

	return output, nil
}
