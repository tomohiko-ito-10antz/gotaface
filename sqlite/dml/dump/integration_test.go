package dump_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/Jumpaku/gotaface/dml"
	schema_impl "github.com/Jumpaku/gotaface/sqlite/ddl/schema"
	"github.com/Jumpaku/gotaface/sqlite/dml/dump"
	"github.com/Jumpaku/gotaface/sqlite/test"
	"golang.org/x/exp/slices"

	_ "github.com/mattn/go-sqlite3"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDumper_Dump(t *testing.T) {

	db, tearDown := test.Setup(t)
	defer tearDown()

	test.Init(t, db, []test.Statement{{
		SQL: `
CREATE TABLE t (
	id2 INT,
	id1 INT,
	col_integer INTEGER,
	col_text TEXT,
	col_real REAL,
	col_blob BLOB,
	PRIMARY KEY (id1, id2));

INSERT INTO t (id1, id2, col_integer, col_text, col_real, col_blob)
VALUES
	(1, 2, 4, "abc", 1.25, X'313234616263'),
	(2, 2, 3, "def", 1.50, X'323233646566'),
	(1, 1, 2, "ghi", 1.75, X'313132676869'),
	(2, 1, 1, "jkl", 2.00, X'3231316a6b6c');
`},
	})

	ctx := context.Background()
	schema, err := schema_impl.NewFetcher(db).Fetch(ctx)
	if err != nil {
		t.Fatalf("fail to fetch schema: %v", err)
	}

	sut := dump.NewDumper(db, schema)

	got, err := sut.Dump(ctx, `t`)
	if err != nil {
		tearDown()
		t.Errorf("fail to dump table: %v", err)
	}
	want := dml.Rows{
		{
			`id1`:         sql.NullInt64{Valid: true, Int64: 1},
			`id2`:         sql.NullInt64{Valid: true, Int64: 1},
			`col_integer`: sql.NullInt64{Valid: true, Int64: 2},
			`col_text`:    sql.NullString{Valid: true, String: `ghi`},
			`col_real`:    sql.NullFloat64{Valid: true, Float64: 1.75},
			`col_blob`:    []byte(`112ghi`),
		}, {
			`id1`:         sql.NullInt64{Valid: true, Int64: 1},
			`id2`:         sql.NullInt64{Valid: true, Int64: 2},
			`col_integer`: sql.NullInt64{Valid: true, Int64: 4},
			`col_text`:    sql.NullString{Valid: true, String: `abc`},
			`col_real`:    sql.NullFloat64{Valid: true, Float64: 1.25},
			`col_blob`:    []byte(`124abc`),
		}, {
			`id1`:         sql.NullInt64{Valid: true, Int64: 2},
			`id2`:         sql.NullInt64{Valid: true, Int64: 1},
			`col_integer`: sql.NullInt64{Valid: true, Int64: 1},
			`col_text`:    sql.NullString{Valid: true, String: `jkl`},
			`col_real`:    sql.NullFloat64{Valid: true, Float64: 2.00},
			`col_blob`:    []byte(`211jkl`),
		}, {
			`id1`:         sql.NullInt64{Valid: true, Int64: 2},
			`id2`:         sql.NullInt64{Valid: true, Int64: 2},
			`col_integer`: sql.NullInt64{Valid: true, Int64: 3},
			`col_text`:    sql.NullString{Valid: true, String: `def`},
			`col_real`:    sql.NullFloat64{Valid: true, Float64: 1.50},
			`col_blob`:    []byte(`223def`),
		},
	}

	if len(got) != len(want) {
		t.Errorf("table count not match\n  len(got) = %v\n  len(want) = %v", len(got), len(want))
	}

	for i, want := range want {
		got := got[i]
		for key, want := range want {
			got, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s", i, key)
			}
			if !equals(got, want) {
				t.Errorf("i = %d, key = %s: got != want\n  gotVal  = %#v\n  wantVal = %#v", i, key, got, want)
			}
		}
	}
}

func equals(got any, want any) bool {
	switch want := want.(type) {
	default:
		return got == want
	case []byte:
		return slices.Equal(got.([]byte), want)
	}
}
