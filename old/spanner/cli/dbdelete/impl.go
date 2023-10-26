package dbdelete

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	spanner_delete "github.com/Jumpaku/gotaface/spanner/dml/delete"
)

type DBDeleteInput = []string

func DBDeleteFunc(ctx context.Context, driver string, dataSource string, input DBDeleteInput) error {
	client, err := spanner.NewClient(ctx, dataSource)
	if err != nil {
		return fmt.Errorf(`fail to create Spanner client %s: %w`, dataSource, err)
	}
	defer client.Close()

	deleter := spanner_delete.NewDeleter(client)
	for _, target := range input {
		err := deleter.Delete(ctx, target)
		if err != nil {
			return fmt.Errorf(`fail to delete rows in table %s: %w`, target, err)
		}
	}

	return nil
}
