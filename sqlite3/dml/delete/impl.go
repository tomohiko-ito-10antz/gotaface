package delete

import (
	"context"
	"fmt"

	"github.com/Jumpaku/gotaface/dbsql"
	"github.com/Jumpaku/gotaface/dml/delete"
)

type deleter struct {
	execer dbsql.Execer
}

var _ delete.Deleter = deleter{}

func NewDeleter(execer dbsql.Execer) deleter {
	return deleter{execer: execer}
}

func (deleter deleter) Delete(ctx context.Context, table string) error {
	_, err := deleter.execer.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, table))
	if err != nil {
		return fmt.Errorf(`fail to delete table: %w`, err)
	}
	return nil
}
