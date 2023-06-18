package dbschema

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	json_schema "github.com/Jumpaku/gotaface/ddl/schema"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
)

type spannerRunner struct {
	dataSource string // not nil
}

func (r *spannerRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	client, err := spanner.NewClient(ctx, r.dataSource)
	if err != nil {
		return fmt.Errorf(`fail to create spanner client: %w`, err)
	}
	defer client.Close()

	fetcher := spanner_schema.NewFetcher(client.Single())
	schema, err := fetcher.Fetch(ctx)
	if err != nil {
		return fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	err = json_schema.WriteSchema(schema, stdout)
	if err != nil {
		return fmt.Errorf(`fail to output schema: %w`, err)
	}

	return nil
}
