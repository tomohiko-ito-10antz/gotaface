package schema_test

import (
	"context"
	"testing"

	"github.com/Jumpaku/gotaface/old/ddl/schema"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/exp/slices"
)

func TestFetcher_Fetch(t *testing.T) {
	input := []byte(`
{
	"tables":[
		{
			"name": "t0",
			"columns": [
				{ "name": "id1", "type": "INT" },
				{ "name": "id2", "type": "INT" },
				{ "name": "col_integer", "type": "INTEGER" },
				{ "name": "col_text", "type": "TEXT" },
				{ "name": "col_real", "type": "REAL" },
				{ "name": "col_blob", "type": "BLOB" }
			],
			"primary_key": [0, 1]
		}, {
			"name": "t1",
			"columns": [
				{ "name": "id", "type": "INT" }
			],
			"primary_key": [0]
		}, {
			"name": "t2",
			"columns": [
				{ "name": "id", "type": "INT" }
			],
			"primary_key": [0]
		}, {
			"name": "t3",
			"columns": [
				{ "name": "id", "type": "INT" },
				{ "name": "col1", "type": "INT" },
				{ "name": "col2", "type": "INT" }
			],
			"primary_key": [0]
		}, {
			"name": "t4",
			"columns": [
				{ "name": "id", "type": "INT" }
			],
			"primary_key": [0]
		}, {
			"name": "t5",
			"columns": [
				{ "name": "id1", "type": "INT" },
				{ "name": "id2", "type": "INT" }
			],
			"primary_key": [0, 1]
		}, {
			"name": "t6",
			"columns": [
				{ "name": "id1", "type": "INT" },
				{ "name": "id2", "type": "INT" },
				{ "name": "id3", "type": "INT" }
			],
			"primary_key": [0, 1, 2]
		}
	],
	"references": [[], [0], [0], [1, 2], [], [4], [5]]
}`)

	sut := schema.NewFetcher(input)

	got, err := sut.Fetch(context.Background())
	if err != nil {
		t.Errorf(`fail to fetch: %v`, err)
	}

	wantTables := []schema.Table{
		schema.TableFormat{
			NameVal: "t0",
			ColumnsVal: []schema.ColumnFormat{
				{NameVal: "id1", TypeVal: "INT"},
				{NameVal: "id2", TypeVal: "INT"},
				{NameVal: "col_integer", TypeVal: "INTEGER"},
				{NameVal: "col_text", TypeVal: "TEXT"},
				{NameVal: "col_real", TypeVal: "REAL"},
				{NameVal: "col_blob", TypeVal: "BLOB"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema.TableFormat{
			NameVal: "t1",
			ColumnsVal: []schema.ColumnFormat{
				{NameVal: "id", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema.TableFormat{
			NameVal: "t2",
			ColumnsVal: []schema.ColumnFormat{
				{NameVal: "id", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema.TableFormat{
			NameVal: "t3",
			ColumnsVal: []schema.ColumnFormat{
				{NameVal: "id", TypeVal: "INT"},
				{NameVal: "col1", TypeVal: "INT"},
				{NameVal: "col2", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema.TableFormat{
			NameVal: "t4",
			ColumnsVal: []schema.ColumnFormat{
				{NameVal: "id", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema.TableFormat{
			NameVal: "t5",
			ColumnsVal: []schema.ColumnFormat{
				{NameVal: "id1", TypeVal: "INT"},
				{NameVal: "id2", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema.TableFormat{
			NameVal: "t6",
			ColumnsVal: []schema.ColumnFormat{
				{NameVal: "id1", TypeVal: "INT"},
				{NameVal: "id2", TypeVal: "INT"},
				{NameVal: "id3", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0, 1, 2},
		},
	}
	wantReferences := [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}}

	gotTables := got.Tables()
	if len(gotTables) != len(wantTables) {
		t.Errorf("table count not match\n  len(gotTables) = %v\n  len(wantTables) = %v", len(gotTables), len(wantTables))
	}

	for i, want := range wantTables {
		if !equalsTable(gotTables[i], want) {
			t.Errorf("Table[%d] = %s not match\n  got = %v\n  want = %v", i, want.Name(), spew.Sdump(gotTables[i]), spew.Sdump(want))
		}
	}

	gotReferences := got.References()
	if !equalsReferences(gotReferences, wantReferences) {
		t.Errorf("References not match\n  got = %#v\n  want = %#v", spew.Sdump(gotReferences), spew.Sdump(wantReferences))
	}
}

func equalsReferences(got [][]int, want [][]int) bool {
	if len(got) != len(want) {
		return false
	}
	for i, got := range got {
		want := want[i]
		if len(got) != len(want) {
			return false
		}
		for j, got := range got {
			want := want[j]
			if got != want {
				return false
			}
		}
	}
	return true
}

func equalsTable(got schema.Table, want schema.Table) bool {
	if got.Name() != want.Name() {
		return false
	}
	if !slices.EqualFunc(got.Columns(), want.Columns(), equalsColumn) {
		return false
	}
	if !slices.Equal(got.PrimaryKey(), want.PrimaryKey()) {
		return false
	}
	return true
}

func equalsColumn(got schema.Column, want schema.Column) bool {
	if got.Name() != want.Name() {
		return false
	}
	if got.Type() != want.Type() {
		return false
	}
	return true
}
