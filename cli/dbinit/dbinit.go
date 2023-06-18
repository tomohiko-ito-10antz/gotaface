package dbinit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/Jumpaku/gotaface/ddl/schema"
	"github.com/Jumpaku/gotaface/dml"
	"github.com/Jumpaku/gotaface/dml/delete"
	"github.com/Jumpaku/gotaface/dml/insert"
	"github.com/Jumpaku/gotaface/topological"
	"golang.org/x/sync/errgroup"
)

type DBInitInput []struct {
	Name string           `json:"name"`
	Rows []map[string]any `json:"rows"`
}

func LoadDBInitInput(reader io.Reader) (DBInitInput, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	decoder.DisallowUnknownFields()

	var input DBInitInput
	if err := decoder.Decode(&input); err != nil {
		return nil, fmt.Errorf(`fail to read JSON from stdin: %w`, err)
	}

	return input, nil
}

type DeleteTablesOrdered [][]string
type InsertTableRowsOrdered []map[string]dml.Rows

func PrepareTableRows(ctx context.Context, schema schema.Schema, input DBInitInput) (DeleteTablesOrdered, InsertTableRowsOrdered, error) {
	schemaTables := schema.Tables()
	schemaReferences := schema.References()
	tableIndex := map[string]int{}
	for index, table := range schema.Tables() {
		tableIndex[table.Name()] = index
	}
	for _, initTable := range input {
		if _, found := tableIndex[initTable.Name]; !found {
			return nil, nil, fmt.Errorf(`table not found: %s`, initTable.Name)
		}
	}
	order, cyclic := topological.Sort(schemaReferences)
	if cyclic {
		return nil, nil, fmt.Errorf(`tables having cyclic reference is not supported`)
	}

	// collect target tables and referenced tables to be deleted
	toBeDeleted := make([]bool, len(schemaTables))
	visited := make([]bool, len(schemaTables))
	children := topological.Transpose(schemaReferences)
	for _, target := range input {
		_ = topological.DFS(children, tableIndex[target.Name], func(v int) error {
			if visited[v] {
				return errors.New("Stop")
			}
			visited[v] = true
			toBeDeleted[v] = true
			return nil
		})
	}
	deleteTablesOrdered := DeleteTablesOrdered{}
	{
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
	}

	// collect target tables to be inserted
	insertTableRowsOrdered := InsertTableRowsOrdered{}
	{
		indices := make([][]int, len(input))
		tableRows := map[string]dml.Rows{}
		for _, toBeInserted := range input {
			tableIndex := tableIndex[toBeInserted.Name]
			indices[order[tableIndex]] = append(indices[order[tableIndex]], tableIndex)

			rows := dml.Rows{}
			for _, insertedRows := range toBeInserted.Rows {
				row := dml.Row{}
				for col, val := range insertedRows {
					row[col] = val
				}
				rows = append(rows, row)
			}
			tableRows[toBeInserted.Name] = rows
		}
		for _, indices := range indices {
			if len(indices) == 0 {
				continue
			}
			tables := map[string]dml.Rows{}
			for _, index := range indices {
				tables[schemaTables[index].Name()] = tableRows[schemaTables[index].Name()]
			}
			insertTableRowsOrdered = append(insertTableRowsOrdered, tables)
		}
	}

	return deleteTablesOrdered, insertTableRowsOrdered, nil
}

func DeleteRowsInParallel(ctx context.Context, deleter delete.Deleter, tables DeleteTablesOrdered) error {
	for _, tables := range tables {
		eg, ctx := errgroup.WithContext(ctx)
		errs := make([]error, len(tables))
		for i, table := range tables {
			i, table := i, table
			eg.Go(func() error {
				err := deleter.Delete(ctx, table)
				if err != nil {
					errs[i] = fmt.Errorf(`fail to delete rows in table %s: %w`, table, err)
				}
				return errs[i]
			})
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf(`fail to delete rows in tables: %w`, errors.Join(errs...))
		}
	}
	return nil
}

func InsertRowsInParallel(ctx context.Context, inserter insert.Inserter, orderedTableRows []map[string]dml.Rows) error {
	for _, tableRows := range orderedTableRows {
		eg, ctx := errgroup.WithContext(ctx)
		errs := map[string]error{}
		for table, rows := range tableRows {
			table, rows := table, rows
			eg.Go(func() error {
				err := inserter.Insert(ctx, table, rows)
				if err != nil {
					errs[table] = fmt.Errorf(`fail to delete rows in table %s: %w`, table, err)
				}
				return errs[table]
			})
		}
		if err := eg.Wait(); err != nil {
			errArr := []error{}
			for _, err := range errs {
				errArr = append(errArr, err)
			}
			return fmt.Errorf(`fail to delete rows in tables: %w`, errors.Join(errArr...))
		}
	}
	return nil
}
