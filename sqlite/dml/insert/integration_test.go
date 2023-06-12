package insert_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/Jumpaku/gotaface/dml"
	"github.com/Jumpaku/gotaface/sqlite/dml/insert"
	"github.com/Jumpaku/gotaface/sqlite/test"

	_ "github.com/mattn/go-sqlite3"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestInserter_Insert(t *testing.T) {
	db, tearDown := test.Setup(t)
	defer tearDown()

	ctx := context.Background()
	_, err := db.ExecContext(ctx, `
CREATE TABLE t (
	id2 INT,
	id1 INT,
	col_integer INTEGER,
	col_text TEXT,
	col_real REAL,
	col_blob BLOB,
	PRIMARY KEY (id1, id2));
`)
	if err != nil {
		tearDown()
		t.Fatalf("fail to create tables: %v", err)
	}
	rows := dml.Rows{
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

	sut := insert.NewInserter(db)
	err = sut.Insert(ctx, `t`, rows)
	if err != nil {
		tearDown()
		t.Errorf("fail to insert rows: %v", err)
	}

	for i, want := range rows {
		got, err := db.QueryContext(ctx, `SELECT id1, id2, col_integer, col_text, col_real, col_blob FROM t WHERE id1 = ? AND id2 = ?`,
			want["id1"].(sql.NullInt64).Int64,
			want["id2"].(sql.NullInt64).Int64,
		)
		if err != nil {
			tearDown()
			t.Fatalf("fail to select row: %v", err)
		}
		if !got.Next() {
			tearDown()
			t.Fatalf("row not found")
		}
		var (
			id1         sql.NullInt64
			id2         sql.NullInt64
			col_integer sql.NullInt64
			col_text    sql.NullString
			col_real    sql.NullFloat64
			col_blob    []byte
		)
		err = got.Scan(&id1, &id2, &col_integer, &col_text, &col_real, &col_blob)
		if err != nil {
			tearDown()
			t.Fatalf("fail to scan row: %v", err)
		}

		if id1 != want["id1"] {
			t.Errorf("i = %d: id1 != want[`id1`]\n  id1  = %#v\n  want['id1'] = %#v", i, id1, want[`id1`])
		}
		if id2 != want["id2"] {
			t.Errorf("i = %d: id2 != want[`id2`]\n  id1  = %#v\n  want['id2'] = %#v", i, id2, want[`id2`])
		}
		if col_integer != want["col_integer"] {
			t.Errorf("i = %d: col_integer != want[`col_integer`]\n  col_integer  = %#v\n  want['col_integer'] = %#v", i, col_integer, want[`col_integer`])
		}
		if col_text != want["col_text"] {
			t.Errorf("i = %d: col_text != want[`col_text`]\n  col_text  = %#v\n  want['col_text'] = %#v", i, col_text, want[`col_text`])
		}
		if string(col_blob) != string(want["col_blob"].([]byte)) {
			t.Errorf("i = %d: col_blob != want[`col_blob`]\n  col_blob  = %#v\n  want['col_blob'] = %#v", i, col_blob, want[`col_blob`])
		}
	}
}
