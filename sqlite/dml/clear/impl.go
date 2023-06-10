package clear

import (
	"context"
	"fmt"

	"github.com/Jumpaku/gotaface/dbsql"
	"github.com/Jumpaku/gotaface/dml/clear"
)

type clearer struct {
	execer dbsql.Execer
}

var _ clear.Clearer = clearer{}

func NewClearer(execer dbsql.Execer) clearer {
	return clearer{execer: execer}
}

func (clearer clearer) Clear(ctx context.Context, table string) error {
	_, err := clearer.execer.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, table))
	if err != nil {
		return fmt.Errorf(`fail to clear table: %w`, err)
	}
	return nil
}
