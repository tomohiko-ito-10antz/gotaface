package insert

import (
	"context"
	"fmt"
	"strings"

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
	if len(rows) == 0 {
		return nil
	}

	keys := []string{}
	for key := range rows[0] {
		keys = append(keys, key)
	}

	values := []byte{}
	params := []any{}
	for n, row := range rows {
		if n > 0 {
			values = append(values, ',')
		}
		values = append(values, '(')
		for i, key := range keys {
			if i > 0 {
				values = append(values, ',')
			}
			values = append(values, '?')
			params = append(params, row[key])
		}
		values = append(values, ')')
	}

	stmt := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s`, table, strings.Join(keys, ","), values)
	_, err := inserter.execer.ExecContext(ctx, stmt, params...)
	if err != nil {
		return fmt.Errorf(`fail to insert rows by %#v [%#v]: %w`, stmt, params, err)
	}
	return nil
}
