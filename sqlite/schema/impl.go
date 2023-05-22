package schema

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Jumpaku/gotaface/dbsql"
	"github.com/Jumpaku/gotaface/schema"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/exp/slices"
	"reflect"
)

type Column struct {
	name string
	typ  string
}

func (c *Column) Name() string {
	return c.name
}
func (c *Column) Type() string {
	return c.typ
}
func (c *Column) GoType() reflect.Type {
	return reflect.TypeOf(nil)
}

type Table struct {
	name    string
	columns []schema.Column
	pk      []int
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Columns() []schema.Column {
	return t.columns
}

func (t *Table) PrimaryKey() []int {
	return t.pk
}

type Schema struct {
	tables     []schema.Table
	references [][]int
}

func (s *Schema) Tables() []schema.Table {
	return s.tables
}

func (s *Schema) References() [][]int {
	return s.references
}

type fetcher struct {
	db *sql.DB
}

func NewFetcher(db *sql.DB) schema.Fetcher {
	return &fetcher{db: db}
}

func (f *fetcher) Fetch(ctx context.Context) (schema.Schema, error) {
	tables, err := f.getTables(ctx)
	if err != nil {
		return nil, fmt.Errorf(`fail to list schema: %w`, err)
	}

	references, err := f.getReferences(ctx, tables)
	if err != nil {
		return nil, fmt.Errorf(`fail to list schema: %w`, err)
	}

	return &Schema{
		tables:     tables,
		references: references,
	}, nil
}

func (f *fetcher) getTables(ctx context.Context) ([]schema.Table, error) {
	type tableColumnRow struct {
		TableName  string
		ColumnName string
		ColumnType string
		PKNumber   int
	}

	rows, err := f.db.QueryContext(ctx, `
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
	scannedRows, err := dbsql.ScanRows(rows, dbsql.NewScanRowTypes[tableColumnRow]())
	if err != nil {
		return nil, fmt.Errorf(`fail to scan rows: %w`, err)
	}

	for _, scannedRow := range scannedRows {
		row := dbsql.StructScanRowValue[tableColumnRow](scannedRow)

		if len(tables) == 0 || tables[len(tables)-1].Name() != row.TableName {
			tables = append(tables, &Table{name: row.TableName})
		}

		table := tables[len(tables)-1].(*Table)
		table.columns = append(table.columns, &Column{
			name: row.ColumnName,
			typ:  row.ColumnType,
		})
		if row.PKNumber > 0 {
			table.pk = append(table.pk, row.PKNumber-1)
		}
	}

	return tables, nil
}

func (f *fetcher) getReferences(ctx context.Context, tables []schema.Table) ([][]int, error) {
	type foreignTableRow struct {
		TableName        string
		ForeignTableName string
	}

	rows, err := f.db.QueryContext(ctx, `
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

	scannedRows, err := dbsql.ScanRows(rows, dbsql.NewScanRowTypes[foreignTableRow]())
	if err != nil {
		return nil, fmt.Errorf(`fail to scan rows: %w`, err)
	}

	for _, scannedRow := range scannedRows {
		row := dbsql.StructScanRowValue[foreignTableRow](scannedRow)
		tableIndex := nameToIndex[row.TableName]
		foreignIndex := nameToIndex[row.ForeignTableName]
		references[tableIndex] = append(references[tableIndex], foreignIndex)
	}

	for _, rs := range references {
		slices.Sort(rs)
	}

	return references, nil
}
