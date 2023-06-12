package insert_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/Jumpaku/gotaface/dml"
	"github.com/Jumpaku/gotaface/sqlite/dml/insert"
	"github.com/Jumpaku/gotaface/sqlite/test"
	"golang.org/x/exp/slices"

	_ "github.com/mattn/go-sqlite3"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestInserter_Insert(t *testing.T) {
	db, tearDown := test.Setup(t)
	defer tearDown()

	test.Init(t, db, []test.Statement{{SQL: `
CREATE TABLE t (
	id2 INT,
	id1 INT,
	col_integer INTEGER,
	col_text TEXT,
	col_real REAL,
	col_blob BLOB,
	PRIMARY KEY (id1, id2));
`}})
	input := dml.Rows{
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

	ctx := context.Background()

	sut := insert.NewInserter(db)

	err := sut.Insert(ctx, `t`, input)
	if err != nil {
		t.Errorf("fail to insert rows: %v", err)
	}

	for i, inputRow := range input {
		type Row struct {
			Id1         sql.NullInt64
			Id2         sql.NullInt64
			Col_integer sql.NullInt64
			Col_text    sql.NullString
			Col_real    sql.NullFloat64
			Col_blob    []byte
		}
		found := test.FindRow[Row](t, db, `t`, map[string]any{"id1": inputRow["id1"].(sql.NullInt64).Int64, "id2": inputRow["id2"].(sql.NullInt64).Int64})
		if found == nil {
			t.Errorf("row not found")
		}

		want := Row{
			Id1:         inputRow["id1"].(sql.NullInt64),
			Id2:         inputRow["id2"].(sql.NullInt64),
			Col_integer: inputRow["col_integer"].(sql.NullInt64),
			Col_text:    inputRow["col_text"].(sql.NullString),
			Col_real:    inputRow["col_real"].(sql.NullFloat64),
			Col_blob:    inputRow["col_blob"].([]byte),
		}

		equals := true
		equals = equals || found.Id1 != want.Id1
		equals = equals || found.Id2 != want.Id2
		equals = equals || found.Col_integer != want.Col_integer
		equals = equals || found.Col_text != want.Col_text
		equals = equals || found.Col_real != want.Col_real
		equals = equals || !slices.Equal(found.Col_blob, want.Col_blob)
		if !equals {
			t.Errorf("i = %d, id1\n found = %#v\n   want = %#v", i, found, want)
		}
	}
}
