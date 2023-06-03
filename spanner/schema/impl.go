package schema

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/schema"
	gotaface_spanner "github.com/Jumpaku/gotaface/spanner"
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
func (c Column) GoType() reflect.Type {
	return reflect.TypeOf(nil)
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
	ParentTables  []*int
	ForeignTables [][]int
}

func (s *Schema) Tables() []schema.Table {
	return s.TablesVal
}

func (s *Schema) References() [][]int {
	references := [][]int{}
	for i, foreignTables := range s.ForeignTables {
		references = append(references, foreignTables)
		if s.ParentTables[i] != nil {
			references[i] = append(references[i], *s.ParentTables[i])
		}
	}
	return references
}

type fetcher struct {
	queryer gotaface_spanner.Queryer
}

func NewFetcher(queryer gotaface_spanner.Queryer) schema.Fetcher {
	return &fetcher{queryer: queryer}
}

func (f *fetcher) Fetch(ctx context.Context) (schema.Schema, error) {
	tables, err := f.getTables(ctx)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch schema: %w`, err)
	}

	parents, foreign, err := f.getReferences(ctx, tables)
	if err != nil {
		return nil, fmt.Errorf(`fail to fetch schema: %w`, err)
	}

	return &Schema{
		TablesVal:     tables,
		ParentTables:  parents,
		ForeignTables: foreign,
	}, nil
}

func (f *fetcher) getTables(ctx context.Context) ([]schema.Table, error) {
	type tableColumnRow struct {
		TableName       string
		Columns         []string
		ColumnPositions []int64
		ColumnTypes     []string
		KeyColumns      []string
		KeyPositions    []int64
	}

	rows := f.queryer.Query(ctx, spanner.Statement{SQL: `
-- Fetches columns and primary keys
WITH c AS (
    SELECT
        c.TABLE_NAME AS TableName, 
        ARRAY_AGG(c.COLUMN_NAME) AS Columns, 
        ARRAY_AGG(c.ORDINAL_POSITION) AS ColumnPositions, 
        ARRAY_AGG(c.SPANNER_TYPE) AS ColumnTypes
    FROM INFORMATION_SCHEMA.TABLES AS t
        JOIN  INFORMATION_SCHEMA.COLUMNS AS c
        ON t.TABLE_NAME = c.TABLE_NAME
    WHERE c.TABLE_CATALOG = '' 
        AND c.TABLE_SCHEMA = ''
        AND t.TABLE_TYPE = 'BASE TABLE' 
        AND c.IS_GENERATED = 'NEVER'
    GROUP BY c.TABLE_NAME
),
p AS (
    SELECT 
        t.TABLE_NAME AS TableName,
        ARRAY_AGG(k.COLUMN_NAME) AS KeyColumns,
        ARRAY_AGG(k.ORDINAL_POSITION) AS KeyPositions
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t 
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k 
        ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
    WHERE
        t.CONSTRAINT_TYPE = 'PRIMARY KEY' 
    GROUP BY t.TABLE_NAME
)

SELECT 
    c.TableName,
    c.Columns,
    c.ColumnPositions,
    c.ColumnTypes,
    p.KeyColumns,
    p.KeyPositions
FROM c JOIN p ON c.TableName = p.TableName
ORDER BY c.TableName;
`})
	scannedRows, err := gotaface_spanner.ScanRows[tableColumnRow](rows)
	if err != nil {
		return nil, fmt.Errorf(`fail to get tables and columns: %w`, err)
	}

	tables := []schema.Table{}
	for _, row := range scannedRows {
		table := &Table{
			NameVal:       row.TableName,
			ColumnsVal:    make([]schema.Column, len(row.Columns)),
			PrimaryKeyVal: make([]int, len(row.KeyColumns)),
		}
		columnPositions := map[string]int{}
		for i, column := range row.Columns {
			table.ColumnsVal[row.ColumnPositions[i]-1] = &Column{
				NameVal: column,
				TypeVal: row.ColumnTypes[i],
			}
			columnPositions[column] = int(row.ColumnPositions[i] - 1)
		}
		for i, key := range row.KeyColumns {
			table.PrimaryKeyVal[row.KeyPositions[i]-1] = columnPositions[key]
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (f *fetcher) getReferences(ctx context.Context, tables []schema.Table) ([]*int, [][]int, error) {
	type referencedTableRow struct {
		TableName         string
		ParentTableName   *string
		ForeignTableNames []string
	}
	rows := f.queryer.Query(ctx, spanner.Statement{SQL: `
-- Fetches parent table and foreign tables
WITH p AS (
    SELECT
        t.TABLE_NAME AS TableName, 
        t.PARENT_TABLE_NAME AS ParentTableName
    FROM INFORMATION_SCHEMA.TABLES AS t
    WHERE t.TABLE_CATALOG = '' 
        AND t.TABLE_SCHEMA = ''
        AND t.TABLE_TYPE = 'BASE TABLE'
),
f AS (
    SELECT 
        t.TABLE_NAME AS TableName,
        ARRAY_AGG(c.TABLE_NAME) AS ForeignTableNames
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
        JOIN INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE AS c
        ON t.CONSTRAINT_NAME = c.CONSTRAINT_NAME
    WHERE t.CONSTRAINT_TYPE = 'FOREIGN KEY'
    GROUP BY TableName
)
SELECT 
    p.TableName,
    p.ParentTableName,
    f.ForeignTableNames
FROM p LEFT OUTER JOIN f ON p.TableName = f.TableName
ORDER BY p.TableName;
`})

	scannedRows, err := gotaface_spanner.ScanRows[referencedTableRow](rows)
	if err != nil {
		return nil, nil, fmt.Errorf(`fail to get foreign tables: %w`, err)
	}

	tableIndex := map[string]int{}
	for index, table := range tables {
		tableIndex[table.Name()] = index
	}

	foreign := make([][]int, len(tables))
	parent := make([]*int, len(tables))
	for _, row := range scannedRows {
		index := tableIndex[row.TableName]
		// parent table
		if row.ParentTableName != nil {
			parentIndex := tableIndex[*row.ParentTableName]
			parent[index] = &parentIndex
		}

		// foreign table
		foreignIndices := map[string]int{}
		for _, foreignTable := range row.ForeignTableNames {
			foreignIndices[foreignTable] = tableIndex[foreignTable]
		}
		for _, foreignIndex := range foreignIndices {
			foreign[index] = append(foreign[index], foreignIndex)
		}
		slices.Sort(foreign[index])
	}

	return parent, foreign, nil
}