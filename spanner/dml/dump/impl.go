package dump

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml"
	"github.com/Jumpaku/gotaface/dml/dump"
	gotaface_spanner "github.com/Jumpaku/gotaface/spanner"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
)

type dumper struct {
	queryer  gotaface_spanner.Queryer
	schema   *spanner_schema.Schema
	tableMap map[string]spanner_schema.Table
}

var _ dump.Dumper = dumper{}

func NewDumper(queryer gotaface_spanner.Queryer, schema *spanner_schema.Schema) dumper {
	tableMap := map[string]spanner_schema.Table{}
	for _, table := range schema.TablesVal {
		tableMap[table.Name()] = table
	}
	return dumper{queryer: queryer, schema: schema, tableMap: tableMap}
}

func (dumper dumper) Dump(ctx context.Context, tableName string) (dml.Rows, error) {
	table, ok := dumper.tableMap[tableName]
	if !ok {
		return nil, fmt.Errorf(`table %s not found`, tableName)
	}

	orderBy := []string{}
	for _, keyIndex := range table.PrimaryKey() {
		orderBy = append(orderBy, table.Columns()[keyIndex].Name())
	}

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT * FROM %s ORDER BY %s`, table.Name(), strings.Join(orderBy, ", ")),
	}

	itr := dumper.queryer.Query(ctx, stmt)

	rows := dml.Rows{}
	err := itr.Do(func(r *spanner.Row) error {
		row := dml.Row{}
		for _, column := range table.ColumnsVal {
			rvPtr := reflect.New(gotaface_spanner.GoType(column.Type()))
			err := r.ColumnByName(column.Name(), rvPtr.Interface())
			if err != nil {
				return fmt.Errorf(`fail to scan column %s : %w`, column.Name(), err)
			}

			row[column.Name()] = rvPtr.Elem().Interface()
		}

		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf(`fail to query by %#v : %w`, stmt, err)
	}

	return rows, nil
}
