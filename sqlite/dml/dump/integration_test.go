package dump_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/Jumpaku/gotaface/dml"
	"github.com/Jumpaku/gotaface/sqlite/dml/dump"
	schema_impl "github.com/Jumpaku/gotaface/sqlite/schema"
	"github.com/Jumpaku/gotaface/sqlite/test"
	"github.com/davecgh/go-spew/spew"

	_ "github.com/mattn/go-sqlite3"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDumper_Dump(t *testing.T) {

	db, tearDown, err := test.Setup()
	if err != nil {
		t.Fatalf(`fail to setup DB for test: %v`, err)
	}
	defer tearDown()

	_, err = db.Exec(`
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
`)
	if err != nil {
		tearDown()
		t.Fatalf("fail to fetch tables: %v", err)
	}

	ctx := context.Background()
	schema, err := schema_impl.NewFetcher(db).Fetch(ctx)
	if err != nil {
		tearDown()
		t.Fatal("fail to fetch tables: %w", err)
	}
	t.Logf(spew.Sdump(schema))

	sut := dump.NewDumper(db, schema)

	got, err := sut.Dump(context.Background(), `t`)
	if err != nil {
		tearDown()
		t.Fatalf("fail to fetch tables: %v", err)
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
		tearDown()
		t.Errorf("table count not match\n  len(got) = %v\n  len(want) = %v", len(got), len(want))
	}

	for i, want := range want {
		{
			key := "id1"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "id2"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_integer"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_text"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_real"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_blob"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if string(gotVal.([]byte)) != string(want[key].([]byte)) {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
	}
}
