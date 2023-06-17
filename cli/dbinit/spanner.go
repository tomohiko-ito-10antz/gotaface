package dbinit

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/ddl/schema"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
	spanner_delete "github.com/Jumpaku/gotaface/spanner/dml/delete"
	spanner_insert "github.com/Jumpaku/gotaface/spanner/dml/insert"
)

type spannerRunner struct {
	dataSource string         // not nil
	fetcher    schema.Fetcher // if nil, fetcher in spanner_schema is used in default
}

func (r *spannerRunner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	client, err := spanner.NewClient(ctx, r.dataSource)
	if err != nil {
		return fmt.Errorf(`fail to create spanner client: %w`, err)
	}
	defer client.Close()

	targets, err := LoadDBInitInput(stdin)
	if err != nil {
		return fmt.Errorf(`fail to load table initialization data from stdin: %w`, err)
	}

	var fetcher = r.fetcher
	if fetcher == nil {
		fetcher = spanner_schema.NewFetcher(client.Single())
	}
	schema, err := fetcher.Fetch(ctx)
	if err != nil {
		return fmt.Errorf(`fail to fetch table schema: %w`, err)
	}

	deleteTables, insertTableRows, err := PrepareTableRows(ctx, schema, targets)
	if err != nil {
		return fmt.Errorf(`fail to prepare tables: %w`, err)
	}

	err = DeleteRowsInParallel(ctx, spanner_delete.NewDeleter(client), deleteTables)
	if err != nil {
		return fmt.Errorf(`fail to delete rows in tables: %w`, err)
	}

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		err = InsertRowsInParallel(ctx, spanner_insert.NewInserter(tx), insertTableRows)
		if err != nil {
			return fmt.Errorf(`fail to insert rows in tables: %w`, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf(`failed read/write transaction: %w`, err)
	}

	return nil
}
