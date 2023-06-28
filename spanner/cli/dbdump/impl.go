package dbdump

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
	spanner_dump "github.com/Jumpaku/gotaface/spanner/dml/dump"
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

	var schema *spanner_schema.Schema
	if schemaReader == nil {
		var err error
		schema, err = spanner_schema.FetchSchema(ctx, rtx)
		if err != nil {
			return nil, fmt.Errorf(`fail to fetch schema: %w`, err)
		}

		if err := json.NewEncoder(schemaWriter).Encode(schema); err != nil {
			return nil, fmt.Errorf(`fail to encode schema JSON: %w`, err)
		}
	} else {
		schema = new(spanner_schema.Schema)
		if err := json.NewDecoder(schemaReader).Decode(schema); err != nil {
			return nil, fmt.Errorf(`fail to decode schema JSON: %w`, err)
		}
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
