package dbschema

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/old/ddl/schema"
	spanner_schema "github.com/Jumpaku/gotaface/old/spanner/ddl/schema"
)

type DBSchemaOutput = interface {
	json.Marshaler
	schema.Schema
}

func DBSchemaFunc(ctx context.Context, driver string, dataSource string) (DBSchemaOutput, error) {
	client, err := spanner.NewClient(ctx, dataSource)
	if err != nil {
		return nil, fmt.Errorf(`fail to create spanner client: %w`, err)
	}
	defer client.Close()

	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	schema, err := spanner_schema.NewFetcher(tx).Fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	return schema.(*spanner_schema.Schema), nil
}
