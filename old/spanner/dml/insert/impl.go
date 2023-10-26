package insert

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/old/dml"
	"github.com/Jumpaku/gotaface/old/dml/insert"
	spanner_impl "github.com/Jumpaku/gotaface/old/spanner"
)

type inserter struct {
	updater spanner_impl.Updater
}

var _ insert.Inserter = inserter{}

func NewInserter(updater spanner_impl.Updater) inserter {
	return inserter{updater: updater}
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
	params := map[string]any{}
	for n, row := range rows {
		if n > 0 {
			values = append(values, ',')
		}
		values = append(values, '(')
		for i, key := range keys {
			if i > 0 {
				values = append(values, ',')
			}

			paramName := key + strconv.FormatInt(int64(n), 10)
			values = append(values, ("@" + paramName)...)
			params[paramName] = row[key]
		}
		values = append(values, ')')
	}

	stmt := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s`, table, strings.Join(keys, ","), values)
	_, err := inserter.updater.Update(ctx, spanner.Statement{SQL: stmt, Params: params})
	if err != nil {
		return fmt.Errorf(`fail to insert rows by %#v [%#v]: %w`, stmt, params, err)
	}
	return nil
}
