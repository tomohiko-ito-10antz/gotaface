package schema

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/Jumpaku/gotaface/dbsql"
	"github.com/Jumpaku/gotaface/ddl/schema"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/exp/slices"
)

type Column struct {
	NameVal string
	TypeVal string
}

func (c Column) Name() string {
	return c.NameVal
}
func (c Column) Type() string {
	return c.TypeVal
}

func RefType[T any]() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}
func GoType(c schema.Column) reflect.Type {
	lower := strings.ToLower(c.Type())
	switch {
	case strings.Contains(lower, "int"):
		return RefType[sql.NullInt64]()
	case strings.Contains(lower, "char"), strings.Contains(lower, "clob"), strings.Contains(lower, "text"):
		return RefType[sql.NullString]()
	case strings.Contains(lower, "blob"), lower == "":
		return RefType[[]byte]()
	case strings.Contains(lower, "real"), strings.Contains(lower, "floa"), strings.Contains(lower, "doub"):
		return RefType[sql.NullFloat64]()
	default:
		return RefType[sql.NullString]()
	}
}

type Table struct {
	NameVal       string
	ColumnsVal    []schema.Column
	PrimaryKeyVal []int
}

func (t Table) Name() string {
	return t.NameVal
}

func (t Table) Columns() []schema.Column {
	return t.ColumnsVal
}

func (t Table) PrimaryKey() []int {
	return t.PrimaryKeyVal
}

type Schema struct {
	TablesVal     []schema.Table
	ReferencesVal [][]int
}

func (s *Schema) Tables() []schema.Table {
	return s.TablesVal
}

func (s *Schema) References() [][]int {
	return s.ReferencesVal
}

type fetcher struct {
	queryer dbsql.Queryer
}

func NewFetcher(db dbsql.Queryer) schema.Fetcher {
	return &fetcher{queryer: db}
}

func (f *fetcher) Fetch(ctx context.Context) (schema.Schema, error) {
	tables, err := f.getTables(ctx)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch schema: %w`, err)
	}

	references, err := f.getReferences(ctx, tables)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch schema: %w`, err)
	}

	return &Schema{
		TablesVal:     tables,
		ReferencesVal: references,
	}, nil
}

func (f *fetcher) getTables(ctx context.Context) ([]schema.Table, error) {
	type tableColumnRow struct {
		TableName  string
		ColumnName string
		ColumnType string
		PKNumber   int
	}

	rows, err := f.queryer.QueryContext(ctx, `
SELECT
    m.name AS TableName,
    c.name AS ColumnName,
    c.type AS ColumnType,
    c.pk AS PKNumber
FROM sqlite_master AS m
JOIN pragma_table_info(m.name) AS c
WHERE m.type = 'table'
ORDER BY m.name, c.cid
`)
	if err != nil {
		return nil, fmt.Errorf(`fail to get tables and columns: %w`, err)
	}
	defer rows.Close()

	tables := []schema.Table{}
	scannedRows, err := dbsql.ScanRowsStruct[tableColumnRow](rows)
	if err != nil {
		return nil, fmt.Errorf(`fail to scan rows: %w`, err)
	}

	for _, row := range scannedRows {
		if len(tables) == 0 || tables[len(tables)-1].Name() != row.TableName {
			tables = append(tables, &Table{NameVal: row.TableName})
		}

		table := tables[len(tables)-1].(*Table)
		table.ColumnsVal = append(table.ColumnsVal, &Column{
			NameVal: row.ColumnName,
			TypeVal: row.ColumnType,
		})
		if row.PKNumber > 0 {
			table.PrimaryKeyVal = append(table.PrimaryKeyVal, row.PKNumber-1)
		}
	}

	return tables, nil
}

func (f *fetcher) getReferences(ctx context.Context, tables []schema.Table) ([][]int, error) {
	type foreignTableRow struct {
		TableName        string
		ForeignTableName string
	}

	rows, err := f.queryer.QueryContext(ctx, `
SELECT
    m.name AS TableName,
    f."table" AS ForeignTableName
FROM sqlite_master AS m
JOIN pragma_foreign_key_list(m.name) AS f
WHERE m.type = 'table'
ORDER BY m.name, f."table"
`)
	if err != nil {
		return nil, fmt.Errorf(`fail to get foreign tables: %w`, err)
	}
	defer rows.Close()

	references := [][]int{}
	nameToIndex := map[string]int{}
	for index, table := range tables {
		references = append(references, []int{})
		nameToIndex[table.Name()] = index
	}

	scannedRows, err := dbsql.ScanRowsStruct[foreignTableRow](rows)
	if err != nil {
		return nil, fmt.Errorf(`fail to scan rows: %w`, err)
	}

	for _, row := range scannedRows {
		tableIndex := nameToIndex[row.TableName]
		foreignIndex := nameToIndex[row.ForeignTableName]
		references[tableIndex] = append(references[tableIndex], foreignIndex)
	}

	for i, rs := range references {
		rsUniq := map[int]any{}
		for _, v := range rs {
			rsUniq[v] = nil
		}
		rs := []int{}
		for v := range rsUniq {
			rs = append(rs, v)
		}
		slices.Sort(rs)
		references[i] = rs
	}

	return references, nil
}
