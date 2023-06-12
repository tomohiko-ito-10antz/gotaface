package schema_test

import (
	"context"
	"testing"

	"github.com/davecgh/go-spew/spew"

	"github.com/Jumpaku/gotaface/ddl/schema"
	schema_impl "github.com/Jumpaku/gotaface/sqlite/ddl/schema"
	"github.com/Jumpaku/gotaface/sqlite/test"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/exp/slices"
)

func TestFetcher_Fetch(t *testing.T) {
	db, tearDown := test.Setup(t)
	defer tearDown()

	ctx := context.Background()
	test.Init(t, db, []test.Statement{{SQL: `
CREATE TABLE t0 (
	id1 INT,
	id2 INT,
	col_integer INTEGER,
	col_text TEXT,
	col_real REAL,
	col_blob BLOB,
	PRIMARY KEY (id1, id2));
	
CREATE TABLE t1 (
	id INT,
	PRIMARY KEY (id),
	FOREIGN KEY (id) REFERENCES t0 (id1));
	
CREATE TABLE t2 (
	id INT,
	PRIMARY KEY (id),
	FOREIGN KEY (id) REFERENCES t0 (id2));
	
CREATE TABLE t3 (
	id INT,
	col1 INT,
	col2 INT,
	PRIMARY KEY (id),
	FOREIGN KEY (col1) REFERENCES t1 (id),
	FOREIGN KEY (col2) REFERENCES t2 (id));
	
CREATE TABLE t4 (
	id INT,
	PRIMARY KEY (id));
	
CREATE TABLE t5 (
	id1 INT,
	id2 INT,
	PRIMARY KEY (id1, id2),
	FOREIGN KEY (id1) REFERENCES t4 (id));

CREATE TABLE t6 (
	id1 INT,
	id2 INT,
	id3 INT,
	PRIMARY KEY (id1, id2, id3),
	FOREIGN KEY (id1, id2) REFERENCES t5 (id1, id2));
`}})

	sut := schema_impl.NewFetcher(db)

	got, err := sut.Fetch(ctx)
	if err != nil {
		t.Errorf("fail to fetch tables: %v", err)
	}

	wantTables := []schema.Table{
		schema_impl.Table{
			NameVal: "t0",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT"},
				schema_impl.Column{NameVal: "id2", TypeVal: "INT"},
				schema_impl.Column{NameVal: "col_integer", TypeVal: "INTEGER"},
				schema_impl.Column{NameVal: "col_text", TypeVal: "TEXT"},
				schema_impl.Column{NameVal: "col_real", TypeVal: "REAL"},
				schema_impl.Column{NameVal: "col_blob", TypeVal: "BLOB"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema_impl.Table{
			NameVal: "t1",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t2",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t3",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id", TypeVal: "INT"},
				schema_impl.Column{NameVal: "col1", TypeVal: "INT"},
				schema_impl.Column{NameVal: "col2", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t4",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t5",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT"},
				schema_impl.Column{NameVal: "id2", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema_impl.Table{
			NameVal: "t6",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT"},
				schema_impl.Column{NameVal: "id2", TypeVal: "INT"},
				schema_impl.Column{NameVal: "id3", TypeVal: "INT"},
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
