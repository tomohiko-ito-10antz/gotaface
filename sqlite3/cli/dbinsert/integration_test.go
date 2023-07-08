package dbinsert_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/dml"
	gotaface_sqlite3 "github.com/Jumpaku/gotaface/sqlite3"
	"github.com/Jumpaku/gotaface/sqlite3/cli/dbinsert"
	sqlite3_schema "github.com/Jumpaku/gotaface/sqlite3/ddl/schema"
	"github.com/Jumpaku/gotaface/sqlite3/test"
	"golang.org/x/exp/slices"
)

var testInitStmt = []test.Statement{
	{SQL: `CREATE TABLE t0 (
		id1 INT,
		id2 INT,
		PRIMARY KEY (id1, id2));`},
	{SQL: `CREATE TABLE t1 (
	col_int INT,
	col_text TEXT,
	col_real REAL,
	col_blob BLOB,
	PRIMARY KEY (col_int));`},
}

var testSchema = &sqlite3_schema.Schema{
	TablesVal: []sqlite3_schema.Table{
		{
			NameVal: `t0`,
			ColumnsVal: []sqlite3_schema.Column{
				{NameVal: `id1`, TypeVal: "INT"},
				{NameVal: `id2`, TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		{
			NameVal: `t1`,
			ColumnsVal: []sqlite3_schema.Column{
				{NameVal: `col_int`, TypeVal: "INT"},
				{NameVal: `col_text`, TypeVal: "TEXT"},
				{NameVal: `col_real`, TypeVal: "REAL"},
				{NameVal: `col_blob`, TypeVal: "BLOB"},
			},
			PrimaryKeyVal: []int{0},
		},
	},
	ReferencesVal: [][]int{{}, {}},
}

type dbInsertRows struct {
	name string
	rows dml.Rows
}

func (r dbInsertRows) Name() string {
	return r.name
}
func (r dbInsertRows) Rows() dml.Rows {
	return r.rows
}

type dbInsertInput []dbInsertRows

func (i dbInsertInput) Len() int {
	return len(i)
}
func (i dbInsertInput) Get(index int) dbinsert.InsertRows {
	return i[index]
}

var testInput = dbInsertInput{
	{
		name: `t0`,
		rows: []dml.Row{
			map[string]any{"id1": 1, "id2": 1},
			map[string]any{"id1": 1, "id2": 2},
			map[string]any{"id1": 2, "id2": 1},
			map[string]any{"id1": 2, "id2": 2},
		},
	},
	{
		name: `t1`,
		rows: []dml.Row{
			map[string]any{
				`col_int`:  3,
				`col_text`: `jkl`,
				`col_real`: 2.00,
				`col_blob`: []byte(`211jkl`),
			},
		},
	},
}

func equals(got any, want any) bool {
	switch want.(type) {
	default:
		return got == want
	case []byte:
		return slices.Equal(got.([]byte), want.([]byte))
	}
}

func getEnvSQLiteTestDirOrSkip(t *testing.T) string {
	t.Helper()

	sqliteTestDir := os.Getenv(test.EnvSQLiteTestDir)
	if sqliteTestDir == "" {
		t.Skipf(`skipped because environment variable %s is not set`, test.EnvSQLiteTestDir)
	}

	return sqliteTestDir
}

func TestDBInsertFunc_WithSchemaCache(t *testing.T) {
	sqliteTestDir := getEnvSQLiteTestDirOrSkip(t)

	dbPath := fmt.Sprintf(`%s/cli_dbinsert_%d.db`, sqliteTestDir, time.Now().UnixNano())
	db, tearDown := test.Setup(t, dbPath, "")
	defer tearDown()

	test.Init(t, db, testInitStmt)

	var schemaReader *bytes.Buffer = bytes.NewBuffer(nil)
	if err := json.NewEncoder(schemaReader).Encode(testSchema); err != nil {
		t.Fatalf(`fail to encode schema to JSON: %v`, err)
	}
	var schemaWriter io.Writer = nil

	// sut
	err := dbinsert.DBInsertFunc(context.Background(), "sqlite3", dbPath, schemaReader, schemaWriter, testInput)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	for _, want := range testInput[0].Rows() {
		key := map[string]any{"id1": want["id1"], "id2": want["id2"]}
		got := test.FindRow[struct {
			Id1 int64
			Id2 int64
		}](t, db, `t0`, key)
		if got == nil {
			t.Errorf("row not found\n  got  = %v\n  want = %v", got, want)
		}
	}
	for _, want := range testInput[1].Rows() {
		var (
			Col_int, _  = gotaface_sqlite3.ToDBValue("INT", want["col_int"])
			Col_text, _ = gotaface_sqlite3.ToDBValue("TEXT", want["col_text"])
			Col_real, _ = gotaface_sqlite3.ToDBValue("REAL", want["col_real"])
			Col_blob, _ = gotaface_sqlite3.ToDBValue("BLOB", want["col_blob"])
		)
		got := test.FindRow[struct {
			Col_int  sql.NullInt64
			Col_text sql.NullString
			Col_real sql.NullFloat64
			Col_blob []byte
		}](t, db, `t1`, map[string]any{"col_int": Col_int})
		if got == nil {
			t.Errorf("row not found\n  got = %v\n  want = %v", got, want)
		}
		eq := equals(got.Col_int, Col_int) &&
			equals(got.Col_text, Col_text) &&
			equals(got.Col_real, Col_real) &&
			equals(got.Col_blob, Col_blob)
		if !eq {
			t.Errorf("row not found\n  got = %v\n  want = %v", got, want)
		}
	}
}

func TestDBInsertFunc_WithoutSchemaCache(t *testing.T) {
	sqliteTestDir := getEnvSQLiteTestDirOrSkip(t)

	dbPath := fmt.Sprintf(`%s/cli_dbinsert_%d.db`, sqliteTestDir, time.Now().UnixNano())
	db, tearDown := test.Setup(t, dbPath, "")
	defer tearDown()

	test.Init(t, db, testInitStmt)

	var schemaReader io.Reader = nil
	var schemaWriter *bytes.Buffer = bytes.NewBuffer(nil)

	// sut
	err := dbinsert.DBInsertFunc(context.Background(), "sqlite3", dbPath, schemaReader, schemaWriter, testInput)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	for _, want := range testInput[0].Rows() {
		key := map[string]any{"id1": want["id1"], "id2": want["id2"]}
		got := test.FindRow[struct {
			Id1 int64
			Id2 int64
		}](t, db, `t0`, key)
		if got == nil {
			t.Errorf("row not found\n  got  = %v\n  want = %v", got, want)
		}
	}
	for _, want := range testInput[1].Rows() {
		var (
			Col_int, _  = gotaface_sqlite3.ToDBValue("INT", want["col_int"])
			Col_text, _ = gotaface_sqlite3.ToDBValue("TEXT", want["col_text"])
			Col_real, _ = gotaface_sqlite3.ToDBValue("REAL", want["col_real"])
			Col_blob, _ = gotaface_sqlite3.ToDBValue("BLOB", want["col_blob"])
		)
		got := test.FindRow[struct {
			Col_int  sql.NullInt64
			Col_text sql.NullString
			Col_real sql.NullFloat64
			Col_blob []byte
		}](t, db, `t1`, map[string]any{"col_int": Col_int})
		if got == nil {
			t.Errorf("row not found\n  got = %v\n  want = %v", got, want)
		}
		eq := equals(got.Col_int, Col_int) &&
			equals(got.Col_text, Col_text) &&
			equals(got.Col_real, Col_real) &&
			equals(got.Col_blob, Col_blob)
		if !eq {
			t.Errorf("row not found\n  got = %v\n  want = %v", got, want)
		}
	}

	var gotSchema sqlite3_schema.Schema
	if err := json.NewDecoder(schemaWriter).Decode(&gotSchema); err != nil {
		t.Errorf(`fail to decode schema: %v`, err)
	}
	{
		wantSchema := testSchema
		if len(gotSchema.TablesVal) != len(wantSchema.TablesVal) {
			t.Errorf("len(got.Tables()) != len(want.TablesVal)\n  got  = %v\n  want = %v", len(gotSchema.Tables()), len(wantSchema.Tables()))
		}
		for i, want := range wantSchema.TablesVal {
			got := gotSchema.TablesVal[i]

			equals := got.Name() == want.Name() &&
				slices.Equal(got.Columns(), want.Columns()) &&
				slices.Equal(got.PrimaryKey(), want.PrimaryKey())

			if !equals {
				t.Errorf("table %d not match\n  got  = %v\n  want = %v", i, got, want)
			}
		}
		if !slices.EqualFunc(gotSchema.References(), wantSchema.References(), func(got, want []int) bool { return slices.Equal(got, want) }) {
			t.Errorf("references not match\n  got  = %v\n  want = %v", gotSchema.References(), wantSchema.References())
		}
	}
}
