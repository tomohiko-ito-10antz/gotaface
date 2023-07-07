package schema

import (
	"context"
	"encoding/json"
	"fmt"

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

type Table struct {
	NameVal       string
	ColumnsVal    []Column
	PrimaryKeyVal []int
}

func (t Table) Name() string {
	return t.NameVal
}

func (t Table) Columns() []schema.Column {
	columns := []schema.Column{}
	for _, column := range t.ColumnsVal {
		columns = append(columns, column)
	}
	return columns
}

func (t Table) PrimaryKey() []int {
	return t.PrimaryKeyVal
}

type Schema struct {
	TablesVal     []Table
	ReferencesVal [][]int
}

type SchemaJSON struct {
	Tables     []TableJSON `json:"tables"`
	References [][]int     `json:"references"`
}
type TableJSON struct {
	Name       string       `json:"name"`
	Columns    []ColumnJSON `json:"columns"`
	PrimaryKey []int        `json:"primary_key"`
}
type ColumnJSON struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

var _ schema.Schema = &Schema{}
var _ json.Marshaler = &Schema{}
var _ json.Unmarshaler = &Schema{}

func (s *Schema) Tables() []schema.Table {
	tables := []schema.Table{}
	for _, table := range s.TablesVal {
		tables = append(tables, table)
	}
	return tables
}

func (s *Schema) References() [][]int {
	return s.ReferencesVal
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	tables := []TableJSON{}
	for _, table := range s.TablesVal {
		columns := []ColumnJSON{}
		for _, column := range table.ColumnsVal {
			columns = append(columns, ColumnJSON{
				Name: column.Name(),
				Type: column.Type(),
			})
		}
		tables = append(tables, TableJSON{
			Name:       table.Name(),
			Columns:    columns,
			PrimaryKey: table.PrimaryKey(),
		})
	}

	b, err := json.Marshal(SchemaJSON{
		Tables:     tables,
		References: s.References(),
	})
	if err != nil {
		return nil, fmt.Errorf(`fail to marshal Schema to JSON: %w`, err)
	}

	return b, nil
}

func (s *Schema) UnmarshalJSON(b []byte) error {
	var schemaJSON SchemaJSON
	if err := json.Unmarshal(b, &schemaJSON); err != nil {
		return fmt.Errorf(`fail to unmarshal Schema from JSON: %w`, err)
	}
	tables := []Table{}
	for _, table := range schemaJSON.Tables {
		columns := []Column{}
		for _, column := range table.Columns {
			columns = append(columns, Column{
				NameVal: column.Name,
				TypeVal: column.Type,
			})
		}
		tables = append(tables, Table{
			NameVal:       table.Name,
			ColumnsVal:    columns,
			PrimaryKeyVal: table.PrimaryKey,
		})
	}
	*s = Schema{
		TablesVal:     tables,
		ReferencesVal: schemaJSON.References,
	}
	return nil
}

type fetcher struct {
	queryer dbsql.Queryer
}

func NewFetcher(db dbsql.Queryer) schema.Fetcher {
	return &fetcher{queryer: db}
}

func FetchSchema(ctx context.Context, queryer dbsql.Queryer) (*Schema, error) {
	tables, err := getTables(ctx, queryer)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch schema: %w`, err)
	}

	references, err := getReferences(ctx, queryer, tables)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch schema: %w`, err)
	}

	return &Schema{
		TablesVal:     tables,
		ReferencesVal: references,
	}, nil
}

func (f *fetcher) Fetch(ctx context.Context) (schema.Schema, error) {
	return FetchSchema(ctx, f.queryer)
}

func getTables(ctx context.Context, queryer dbsql.Queryer) ([]Table, error) {
	type tableColumnRow struct {
		TableName  string
		ColumnName string
		ColumnType string
		PKNumber   int
	}

	rows, err := queryer.QueryContext(ctx, `
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

	tables := []Table{}
	scannedRows, err := dbsql.ScanRowsStruct[tableColumnRow](rows)
	if err != nil {
		return nil, fmt.Errorf(`fail to scan rows: %w`, err)
	}

	for _, row := range scannedRows {
		if len(tables) == 0 || tables[len(tables)-1].Name() != row.TableName {
			tables = append(tables, Table{NameVal: row.TableName})
		}

		table := &tables[len(tables)-1]
		table.ColumnsVal = append(table.ColumnsVal, Column{
			NameVal: row.ColumnName,
			TypeVal: row.ColumnType,
		})
		if row.PKNumber > 0 {
			table.PrimaryKeyVal = append(table.PrimaryKeyVal, row.PKNumber-1)
		}
	}

	return tables, nil
}

func getReferences(ctx context.Context, queryer dbsql.Queryer, tables []Table) ([][]int, error) {
	type foreignTableRow struct {
		TableName        string
		ForeignTableName string
	}

	rows, err := queryer.QueryContext(ctx, `
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
