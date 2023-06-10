package main

import (
	"context"
	"log"

	"cloud.google.com/go/spanner"
)

func main() {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, "projects/gotaface/instances/test/databases/example")
	if err != nil {
		log.Fatalln(err)
	}
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if n, err := tx.Update(ctx, spanner.Statement{
			SQL: `
UPDATE t SET 
	col2 = 8
WHERE col1 = 5
`}); err != nil {
			return err
		} else if n == 0 {
			if _, err := tx.Update(ctx, spanner.Statement{
				SQL: `
INSERT INTO t (
	col1,
	col2
) VALUES (
	5, 
	5
) 
`}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}
}
