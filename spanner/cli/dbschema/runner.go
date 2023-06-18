package dbschema

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	json_schema "github.com/Jumpaku/gotaface/ddl/schema"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
)

type SpannerRunner struct {
	DataSource string // not nil
}

func (r *SpannerRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	client, err := spanner.NewClient(ctx, r.DataSource)
	if err != nil {
		return fmt.Errorf(`fail to create spanner client: %w`, err)
	}
	defer client.Close()

	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	fetcher := spanner_schema.NewFetcher(tx)
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
