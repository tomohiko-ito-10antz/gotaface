package schema_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/Jumpaku/gotaface/old/ddl/schema"
	schema_impl "github.com/Jumpaku/gotaface/old/spanner/ddl/schema"
	spanner_test "github.com/Jumpaku/gotaface/old/spanner/test"

	"golang.org/x/exp/slices"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestFetcher_Fetch(t *testing.T) {
	adminClient, client, tearDown := spanner_test.Setup(t, fmt.Sprintf(`ddl_schema_%d`, time.Now().UnixNano()))
	defer tearDown()

	spanner_test.InitDDL(t, adminClient, client.DatabaseName(), []string{`
CREATE TABLE t0 (
	id1 INT64,
	id2 INT64,
	col_integer INT64,
	col_string STRING(MAX),
	col_float FLOAT64,
	col_bytes BYTES(16),
	col_bool BOOL,
	col_date DATE,
	col_timestamp TIMESTAMP
) PRIMARY KEY (id1, id2)`,
		`
CREATE TABLE t1 (
	id INT64,
	FOREIGN KEY (id) REFERENCES t0 (id1)
) PRIMARY KEY (id)`,
		`
CREATE TABLE t2 (
	id INT64,
	FOREIGN KEY (id) REFERENCES t0 (id2)
) PRIMARY KEY (id)`,
		`
CREATE TABLE t3 (
	id INT64,
	col1 INT64,
	col2 INT64,
	FOREIGN KEY (col1) REFERENCES t1 (id),
	FOREIGN KEY (col2) REFERENCES t2 (id)
) PRIMARY KEY (id)`,
		`
CREATE TABLE t4 (
	id1 INT64
) PRIMARY KEY (id1)`,
		`
CREATE TABLE t5 (
	id1 INT64,
	id2 INT64,
	FOREIGN KEY (id1) REFERENCES t4 (id1)
) PRIMARY KEY (id1, id2)`,
		`
CREATE TABLE t6 (
	id1 INT64,
	id2 INT64,
	id3 INT64,
	FOREIGN KEY (id1, id2) REFERENCES t5 (id1, id2)
) PRIMARY KEY (id1, id2, id3)`,
		`
CREATE TABLE t7 (
	id1 INT64
) PRIMARY KEY (id1)`,
		`
CREATE TABLE t8 (
	id1 INT64,
	id2 INT64
) PRIMARY KEY (id1, id2),
	INTERLEAVE IN PARENT t7`,
		`
CREATE TABLE t9 (
	id1 INT64,
	id2 INT64,
	id3 INT64
) PRIMARY KEY (id1, id2, id3),
	INTERLEAVE IN PARENT t8`,
	})
	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	sut := schema_impl.NewFetcher(tx)

	got, err := sut.Fetch(context.Background())
	if err != nil {
		t.Fatalf("fail to fetch tables: %v", err)
	}

	wantTables := []schema.Table{
		schema_impl.Table{
			NameVal: "t0",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
				{NameVal: "col_integer", TypeVal: "INT64"},
				{NameVal: "col_string", TypeVal: "STRING(MAX)"},
				{NameVal: "col_float", TypeVal: "FLOAT64"},
				{NameVal: "col_bytes", TypeVal: "BYTES(16)"},
				{NameVal: "col_bool", TypeVal: "BOOL"},
				{NameVal: "col_date", TypeVal: "DATE"},
				{NameVal: "col_timestamp", TypeVal: "TIMESTAMP"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema_impl.Table{
			NameVal: "t1",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t2",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t3",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id", TypeVal: "INT64"},
				{NameVal: "col1", TypeVal: "INT64"},
				{NameVal: "col2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t4",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id1", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t5",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema_impl.Table{
			NameVal: "t6",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
				{NameVal: "id3", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1, 2},
		},
		schema_impl.Table{
			NameVal: "t7",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id1", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t8",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema_impl.Table{
			NameVal: "t9",
			ColumnsVal: []schema_impl.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
				{NameVal: "id3", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1, 2},
		},
	}
	wantReferences := [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}, {}, {7}, {8}}

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
	if !slices.EqualFunc(got.Columns(), want.Columns(), func(got, want schema.Column) bool {
		return equalsColumn(got, want)
	}) {
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
