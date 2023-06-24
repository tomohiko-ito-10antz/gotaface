package dbinit_test

import (
	"testing"

	"github.com/Jumpaku/gotaface/cli/dbinit"
	json_schema "github.com/Jumpaku/gotaface/ddl/schema"
	"golang.org/x/exp/slices"
)

func TestPrepareTableRows(t *testing.T) {
	schema := json_schema.SchemaFormat{
		TablesVal: []json_schema.TableFormat{
			{
				NameVal: "t0",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1},
			},
			{
				NameVal: "t1",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t2",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t3",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT64"},
					{NameVal: "col1", TypeVal: "INT64"},
					{NameVal: "col2", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t4",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t5",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1},
			},
			{
				NameVal: "t6",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
					{NameVal: "id3", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1, 2},
			},
			{
				NameVal: "t7",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t8",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1},
			},
			{
				NameVal: "t9",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
					{NameVal: "id3", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1, 2},
			},
		},
		ReferencesVal: [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}, {}, {7}, {8}},
	}
	input := dbinit.DBInitInput{
		{
			Name: `t0`,
			Rows: []map[string]any{
				{},
			},
		},
		{
			Name: `t4`,
		},
		{
			Name: `t5`,
		},
		{
			Name: `t6`,
		},
		{
			Name: `t9`,
		},
	}

	wantDel := dbinit.DeleteTablesOrdered{}
	wantIns := dbinit.InsertTableRowsOrdered{}

	gotDel, gotIns, err := dbinit.PrepareTableRows(schema, input)
	if err != nil {
		t.Errorf(`fail to prepare table rows: %v`, err)
	}

	for i, want := range wantDel {
		got := gotDel[i]
		if !slices.Equal(got, want) {
			t.Errorf("i = %d tables to be deleted does not match\n  got  = %#v\n  want = %#v", i, got, want)
		}
	}
	for i, want := range wantIns {
		got := gotIns[i]
		for table, want := range want {
			got, ok := got[table]
			if !ok {
				t.Errorf("i = %d table rows to be inserted have no table %v", i, table)
			}
			for j, want := range want {
				got := got[j]
				for column, want := range want {
					got, ok := got[column]
					if !ok {
						t.Errorf("i = %d, j = %d table row to be inserted have no column %v", i, j, column)
					}
					if got != want {
						t.Errorf("i = %d j = %d tables to be delete are not match\n  got  = %#v\n  want = %#v", i, j, got, want)
					}
				}
			}
		}
	}
}
