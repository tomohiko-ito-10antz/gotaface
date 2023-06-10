package insert

import (
	"context"
	"fmt"

	"github.com/Jumpaku/gotaface/dbsql"
	"github.com/Jumpaku/gotaface/dml"
	"github.com/Jumpaku/gotaface/dml/insert"
)

type inserter struct {
	execer dbsql.Execer
}

var _ insert.Inserter = inserter{}

func NewInserter(execer dbsql.Execer) inserter {
	return inserter{execer: execer}
}

func (inserter inserter) Insert(ctx context.Context, table string, rows dml.Rows) error {
	_, err := inserter.execer.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, table))
	if err != nil {
		return fmt.Errorf(`fail to insert table: %w`, err)
	}
	return nil
}
