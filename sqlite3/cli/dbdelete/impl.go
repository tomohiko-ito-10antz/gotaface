package dbdelete

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/Jumpaku/gotaface/dbsql"
	sqlite3_schema "github.com/Jumpaku/gotaface/sqlite3/ddl/schema"
	sqlite3_delete "github.com/Jumpaku/gotaface/sqlite3/dml/delete"
	"github.com/Jumpaku/gotaface/topological"
	"golang.org/x/sync/errgroup"
)

type DBDeleteInput = []string

func DBDeleteFunc(ctx context.Context, driver string, dataSource string, schemaReader io.Reader, schemaWriter io.Writer, input DBDeleteInput) error {
	db, err := sql.Open("sqlite3", dataSource)
	if err != nil {
		return fmt.Errorf(`fail to open SQLite3 client %s: %w`, dataSource, err)
	}
	defer db.Close()

	var schema *sqlite3_schema.Schema
	if schemaReader == nil { // cache not found
		fetcher := sqlite3_schema.NewFetcher(db)
		s, err := fetcher.Fetch(ctx)
		if err != nil {
			return fmt.Errorf(`fail to fetch table schema: %w`, err)
		}
		schema = s.(*sqlite3_schema.Schema)

		if err = WriteSchemaJSON(schema, schemaWriter); err != nil {
			return fmt.Errorf(`fail to save table schema: %w`, err)
		}
	} else { // cache
		var err error
		schema, err = ReadSchemaJSON(schemaReader)
		if err != nil {
			return fmt.Errorf(`fail to load schema cache file: %w`, err)
		}
	}

	targets := input

	deleteTables, err := prepareDeleteTable(schema, targets)
	if err != nil {
		return fmt.Errorf(`fail to prepare tables: %w`, err)
	}

	err = deleteRowsInParallel(ctx, db, deleteTables)
	if err != nil {
		return fmt.Errorf(`fail to delete rows in tables: %w`, err)
	}

	return nil
}

func ReadSchemaJSON(reader io.Reader) (*sqlite3_schema.Schema, error) {
	schema := sqlite3_schema.Schema{}
	if err := json.NewDecoder(reader).Decode(&schema); err != nil {
		return nil, fmt.Errorf(`fail to unmarshal JSON: %w`, err)
	}

	return &schema, nil

}

func WriteSchemaJSON(schema *sqlite3_schema.Schema, writer io.Writer) error {
	if err := json.NewEncoder(writer).Encode(schema); err != nil {
		return fmt.Errorf(`fail to encode JSON: %w`, err)
	}

	return nil

}

type DeleteTablesOrdered [][]string

func prepareDeleteTable(schema *sqlite3_schema.Schema, targets DBDeleteInput) (DeleteTablesOrdered, error) {
	tableIndex := map[string]int{}
	for index, table := range schema.Tables() {
		tableIndex[table.Name()] = index
	}

	for _, table := range targets {
		if _, found := tableIndex[table]; !found {
			return nil, fmt.Errorf(`table not found: %s`, table)
		}
	}

	order, ok := topological.Sort(schema.References())
	if !ok {
		return nil, fmt.Errorf(`tables with cyclic reference are not supported`)
	}

	// collect target tables and referenced tables to be deleted
	deleteTablesOrdered := collectDeleteTables(schema.TablesVal, schema.References(), tableIndex, order, targets)

	return deleteTablesOrdered, nil
}

func collectDeleteTables(schemaTables []sqlite3_schema.Table, schemaReferences [][]int, tableIndex map[string]int, order []int, targets []string) DeleteTablesOrdered {
	toBeDeleted := make([]bool, len(schemaTables))
	visited := make([]bool, len(schemaTables))
	children := topological.Transpose(schemaReferences)
	for _, target := range targets {
		_ = topological.DFS(children, tableIndex[target], func(v int) error {
			if visited[v] {
				return errors.New("Stop")
			}
			visited[v] = true
			toBeDeleted[v] = true
			return nil
		})
	}

	deleteTablesOrdered := DeleteTablesOrdered{}
	indices := make([][]int, len(toBeDeleted))
	for tableIndex, toBeDeleted := range toBeDeleted {
		if toBeDeleted {
			indices[order[tableIndex]] = append(indices[order[tableIndex]], tableIndex)
		}
	}
	for _, indices := range indices {
		if len(indices) == 0 {
			continue
		}
		tables := []string{}
		for _, index := range indices {
			tables = append(tables, schemaTables[index].Name())
		}
		deleteTablesOrdered = append(deleteTablesOrdered, tables)
	}

	return deleteTablesOrdered
}

type DB interface {
	dbsql.Execer
	dbsql.Queryer
}

func deleteRowsInParallel(ctx context.Context, db DB, tables DeleteTablesOrdered) (err error) {
	var foreignKeys bool
	err = db.QueryRowContext(ctx, `SELECT foreign_keys AS ForeignKeys FROM pragma_foreign_keys()`).Scan(&foreignKeys)
	if err != nil {
		return fmt.Errorf(`fail to check whether foreign key is active: %w`, err)
	}
	if foreignKeys {
		for _, tables := range tables {
			eg, ctx := errgroup.WithContext(ctx)
			for _, table := range tables {
				table := table
				eg.Go(func() error {
					if err := sqlite3_delete.NewDeleter(db).Delete(ctx, table); err != nil {
						return fmt.Errorf(`fail to delete rows in table %s: %w`, table, err)
					}
					return nil
				})
			}
			if err := eg.Wait(); err != nil {
				return fmt.Errorf(`fail to delete rows in tables: %w`, err)
			}
		}
	} else {
		eg, ctx := errgroup.WithContext(ctx)
		for _, tables := range tables {
			for _, table := range tables {
				table, execer := table, db
				eg.Go(func() error {
					if err := sqlite3_delete.NewDeleter(execer).Delete(ctx, table); err != nil {
						return fmt.Errorf(`fail to delete rows in table %s: %w`, table, err)
					}
					return nil
				})
			}
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf(`fail to delete rows in tables: %w`, err)
		}
	}

	return nil
}
