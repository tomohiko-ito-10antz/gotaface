package dbschema

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
)

type SpannerRunner struct {
	DataSource string // not nil
}

type DBSchemaOutput struct {
	DataSource string
	Tables     []spanner_schema.Table
	References [][]int
}

func (r *SpannerRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	client, err := spanner.NewClient(ctx, r.DataSource)
	if err != nil {
		return fmt.Errorf(`fail to create spanner client: %w`, err)
	}
	defer client.Close()

	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	schema, err := spanner_schema.NewFetcher(tx).Fetch(ctx)
	if err != nil {
		return fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	if err := json.NewEncoder(stdout).Encode(schema.(*spanner_schema.Schema)); err != nil {
		return fmt.Errorf(`fail to encode table schema as JSON: %w`, err)
	}

	return nil
}
