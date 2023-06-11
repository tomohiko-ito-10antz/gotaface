package schema_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/Jumpaku/gotaface/ddl/schema"
	schema_impl "github.com/Jumpaku/gotaface/spanner/ddl/schema"
	spanner_test "github.com/Jumpaku/gotaface/spanner/test"

	spanner_adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"golang.org/x/exp/slices"
)

var (
	testSpannerProject  string
	testSpannerInstance string
)
var skipTest bool

func TestMain(m *testing.M) {
	initialize()
	os.Exit(m.Run())
}

func initialize() {
	if testSpannerProject = os.Getenv(spanner_test.EnvTestSpannerProject); testSpannerProject == "" {
		skipTest = true
		return
	}
	if testSpannerInstance = os.Getenv(spanner_test.EnvTestSpannerInstance); testSpannerInstance == "" {
		skipTest = true
		return
	}
}

func newDatabaseName() string {
	return fmt.Sprintf(`schema_%d`, time.Now().Unix())
}

func TestFetcher_Fetch(t *testing.T) {
	if skipTest {
		t.Skip()
	}
	spannerAdminClient, spannerClient, tearDown, err := spanner_test.Setup(testSpannerProject, testSpannerInstance, newDatabaseName())
	if err != nil {
		t.Fatalf(`fail to set up spanner: %v`, err)
	}
	defer tearDown()

	ctx := context.Background()
	ddl := &spanner_adminpb.UpdateDatabaseDdlRequest{
		Database: spannerClient.DatabaseName(),
		Statements: []string{
			`
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
		},
	}
	op, err := spannerAdminClient.UpdateDatabaseDdl(ctx, ddl)
	if err != nil {
		tearDown()
		t.Fatalf(`fail to wait create tables: %v`, err)
	}
	if err := op.Wait(ctx); err != nil {
		tearDown()
		t.Fatalf(`fail to wait create tables: %v`, err)
	}

	tx := spannerClient.ReadOnlyTransaction()

	sut := schema_impl.NewFetcher(tx)

	got, err := sut.Fetch(context.Background())
	if err != nil {
		tx.Close()
		tearDown()
		t.Fatal("fail to fetch tables: %w", err)
	}
	wantTables := []schema.Table{
		schema_impl.Table{
			NameVal: "t0",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "id2", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "col_integer", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "col_string", TypeVal: "STRING(MAX)"},
				schema_impl.Column{NameVal: "col_float", TypeVal: "FLOAT64"},
				schema_impl.Column{NameVal: "col_bytes", TypeVal: "BYTES(16)"},
				schema_impl.Column{NameVal: "col_bool", TypeVal: "BOOL"},
				schema_impl.Column{NameVal: "col_date", TypeVal: "DATE"},
				schema_impl.Column{NameVal: "col_timestamp", TypeVal: "TIMESTAMP"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema_impl.Table{
			NameVal: "t1",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t2",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t3",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "col1", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "col2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t4",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t5",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "id2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema_impl.Table{
			NameVal: "t6",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "id2", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "id3", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1, 2},
		},
		schema_impl.Table{
			NameVal: "t7",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		schema_impl.Table{
			NameVal: "t8",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "id2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		schema_impl.Table{
			NameVal: "t9",
			ColumnsVal: []schema.Column{
				schema_impl.Column{NameVal: "id1", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "id2", TypeVal: "INT64"},
				schema_impl.Column{NameVal: "id3", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1, 2},
		},
	}
	wantReferences := [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}, {}, {7}, {8}}

	gotTables := got.Tables()
	if len(gotTables) != len(wantTables) {
		tx.Close()
		tearDown()
		t.Errorf("table count not match\n  len(gotTables) = %v\n  len(wantTables) = %v", len(gotTables), len(wantTables))
	}

	for i, want := range wantTables {
		if !equalsTable(t, gotTables[i], want) {
			tx.Close()
			tearDown()
			t.Errorf("Table[%d] = %s not match\n  got = %v\n  want = %v", i, want.Name(), spew.Sdump(gotTables[i]), spew.Sdump(want))
		}
	}

	gotReferences := got.References()
	if !equalsReferences(t, gotReferences, wantReferences) {
		tx.Close()
		tearDown()
		t.Errorf("References not match\n  got = %#v\n  want = %#v", spew.Sdump(gotReferences), spew.Sdump(wantReferences))
	}
}

func equalsReferences(t *testing.T, got [][]int, want [][]int) bool {
	t.Helper()

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
func equalsTable(t *testing.T, got schema.Table, want schema.Table) bool {
	t.Helper()

	if got.Name() != want.Name() {
		return false
	}
	if !slices.EqualFunc(got.Columns(), want.Columns(), func(got, want schema.Column) bool {
		return equalsColumn(t, got, want)
	}) {
		return false
	}
	if !slices.Equal(got.PrimaryKey(), want.PrimaryKey()) {
		return false
	}
	return true
}

func equalsColumn(t *testing.T, got schema.Column, want schema.Column) bool {
	t.Helper()

	if got.Name() != want.Name() {
		return false
	}
	if got.Type() != want.Type() {
		return false
	}
	return true
}
