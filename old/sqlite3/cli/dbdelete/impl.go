package dbdelete

import (
	"context"
	"database/sql"
	"fmt"

	sqlite3_delete "github.com/Jumpaku/gotaface/sqlite3/dml/delete"
)

type DBDeleteInput = []string

func DBDeleteFunc(ctx context.Context, driver string, dataSource string, input DBDeleteInput) error {
	db, err := sql.Open("sqlite3", dataSource)
	if err != nil {
		return fmt.Errorf(`fail to open SQLite3 client %s: %w`, dataSource, err)
	}
	defer db.Close()

	deleter := sqlite3_delete.NewDeleter(db)
	for _, target := range input {
		if err := deleter.Delete(ctx, target); err != nil {
			return fmt.Errorf(`fail to delete rows in table %s: %w`, target, err)
		}
	}

	return nil
}
