package dump

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml"
	"github.com/Jumpaku/gotaface/dml/dump"
	"github.com/Jumpaku/gotaface/schema"
	gotaface_spanner "github.com/Jumpaku/gotaface/spanner"
	schema_impl "github.com/Jumpaku/gotaface/spanner/schema"
	"golang.org/x/exp/slices"
)

type dumper struct {
	queryer gotaface_spanner.Queryer
	schema  schema.Schema
}

var _ dump.Dumper = dumper{}

func NewDumper(queryer gotaface_spanner.Queryer, schema schema.Schema) dumper {
	return dumper{queryer: queryer, schema: schema}
}

func (dumper dumper) Dump(ctx context.Context, tableName string) (dml.Rows, error) {
	table, orderBy, err := dumper.getTableInfo(tableName)
	if err != nil {
		return nil, fmt.Errorf(`table not found %#v : %w`, tableName, err)
	}

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT * FROM %s ORDER BY %s`, table.Name(), strings.Join(orderBy, ", ")),
	}

	itr := dumper.queryer.Query(ctx, stmt)

	rows := dml.Rows{}
	err = itr.Do(func(r *spanner.Row) error {
		row := dml.Row{}
		for _, column := range table.Columns() {
			rvPtr := reflect.New(schema_impl.GoType(column))
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

func (dumper dumper) getTableInfo(tableName string) (schema.Table, []string, error) {
	tables := dumper.schema.Tables()
	index := slices.IndexFunc(tables, func(t schema.Table) bool { return t.Name() == tableName })
	if index < 0 {
		return nil, nil, fmt.Errorf(`table %s not found`, tableName)
	}
	table := tables[index]

	orderBy := []string{}
	for _, keyIndex := range table.PrimaryKey() {
		orderBy = append(orderBy, table.Columns()[keyIndex].Name())
	}

	return table, orderBy, nil
}
