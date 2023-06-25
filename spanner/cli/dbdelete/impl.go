package dbdelete

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml/delete"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
	spanner_delete "github.com/Jumpaku/gotaface/spanner/dml/delete"
	"github.com/Jumpaku/gotaface/topological"
	"golang.org/x/sync/errgroup"
)

type DBDeleteInput = []string

func DBDeleteFunc(ctx context.Context, driver string, dataSource string, schemaReader io.Reader, schemaWriter io.Writer, input DBDeleteInput) error {
	client, err := spanner.NewClient(ctx, dataSource)
	if err != nil {
		return fmt.Errorf(`fail to create Spanner client %s: %w`, dataSource, err)
	}
	defer client.Close()

	var schema *spanner_schema.Schema
	if schemaReader == nil { // cache not found
		tx := client.ReadOnlyTransaction()
		defer tx.Close()
		fetcher := spanner_schema.NewFetcher(tx)
		s, err := fetcher.Fetch(ctx)
		if err != nil {
			return fmt.Errorf(`fail to fetch table schema: %w`, err)
		}
		schema = s.(*spanner_schema.Schema)

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

	err = deleteRowsInParallel(ctx, spanner_delete.NewDeleter(client), deleteTables)
	if err != nil {
		return fmt.Errorf(`fail to delete rows in tables: %w`, err)
	}

	return nil
}

func ReadSchemaJSON(reader io.Reader) (*spanner_schema.Schema, error) {
	schema := spanner_schema.Schema{}
	if err := json.NewDecoder(reader).Decode(&schema); err != nil {
		return nil, fmt.Errorf(`fail to unmarshal JSON: %w`, err)
	}

	return &schema, nil

}

func WriteSchemaJSON(schema *spanner_schema.Schema, writer io.Writer) error {
	if err := json.NewEncoder(writer).Encode(schema); err != nil {
		return fmt.Errorf(`fail to encode JSON: %w`, err)
	}

	return nil

}

type DeleteTablesOrdered [][]string

func prepareDeleteTable(schema *spanner_schema.Schema, targets DBDeleteInput) (DeleteTablesOrdered, error) {
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

func collectDeleteTables(schemaTables []spanner_schema.Table, schemaReferences [][]int, tableIndex map[string]int, order []int, targets []string) DeleteTablesOrdered {
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

func deleteRowsInParallel(ctx context.Context, deleter delete.Deleter, tables DeleteTablesOrdered) error {
	for _, tables := range tables {
		eg, ctx := errgroup.WithContext(ctx)
		for _, table := range tables {
			table := table
			eg.Go(func() error {
				if err := deleter.Delete(ctx, table); err != nil {
					return fmt.Errorf(`fail to delete rows in table %s: %w`, table, err)
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf(`fail to delete rows in tables: %w`, err)
		}
	}
	return nil
}
