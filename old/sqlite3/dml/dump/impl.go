package dump

import (
	"context"
	"fmt"
	"strings"

	"github.com/Jumpaku/gotaface/old/dbsql"
	"github.com/Jumpaku/gotaface/old/ddl/schema"
	"github.com/Jumpaku/gotaface/old/dml"
	"github.com/Jumpaku/gotaface/old/dml/dump"
	"github.com/Jumpaku/gotaface/old/sqlite3"
	sqlite3_schema "github.com/Jumpaku/gotaface/old/sqlite3/ddl/schema"
	"golang.org/x/exp/slices"
)

type dumper struct {
	queryer dbsql.Queryer
	schema  *sqlite3_schema.Schema
}

var _ dump.Dumper = dumper{}

func NewDumper(queryer dbsql.Queryer, schema *sqlite3_schema.Schema) dumper {
	return dumper{queryer: queryer, schema: schema}
}

func (dumper dumper) Dump(ctx context.Context, tableName string) (dml.Rows, error) {
	table, orderBy, scanTypes, err := dumper.getTableInfo(tableName)
	if err != nil {
		return nil, fmt.Errorf(`table not found %#v : %w`, tableName, err)
	}

	stmt := fmt.Sprintf(`SELECT * FROM %s ORDER BY %s`, table.Name(), strings.Join(orderBy, ", "))
	result, err := dumper.queryer.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf(`fail to query by %#v : %w`, stmt, err)
	}

	scanRows, err := dbsql.ScanRows(result, scanTypes)
	if err != nil {
		return nil, fmt.Errorf(`fail to scan dumped table: %w`, err)
	}

	rows := dml.Rows{}
	for _, scanRow := range scanRows {
		row := dml.Row{}
		for col, val := range scanRow {
			row[col] = val
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (dumper dumper) getTableInfo(tableName string) (schema.Table, []string, dbsql.ScanRowTypes, error) {
	tables := dumper.schema.Tables()
	index := slices.IndexFunc(tables, func(t schema.Table) bool { return t.Name() == tableName })
	if index < 0 {
		return nil, nil, nil, fmt.Errorf(`table %s not found`, tableName)
	}
	table := tables[index]

	orderBy := []string{}
	for _, keyIndex := range table.PrimaryKey() {
		orderBy = append(orderBy, table.Columns()[keyIndex].Name())
	}

	scanTypes := dbsql.ScanRowTypes{}
	for _, column := range table.Columns() {
		scanTypes[column.Name()] = sqlite3.GoType(column.Type())
	}

	return table, orderBy, scanTypes, nil
}
