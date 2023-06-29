package dbdump_test

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
	"github.com/Jumpaku/gotaface/sqlite3/cli/dbdump"
	sqlite3_schema "github.com/Jumpaku/gotaface/sqlite3/ddl/schema"
	"github.com/Jumpaku/gotaface/sqlite3/test"
	"golang.org/x/exp/slices"
)

var testInitStmt = []test.Statement{
	{SQL: `CREATE TABLE t0 (
	col_int INT,
	col_text TEXT,
	col_real REAL,
	col_blob BLOB,
	PRIMARY KEY (col_int));`},
	{SQL: `CREATE TABLE t1 (
	id1 INT,
	id2 INT,
	PRIMARY KEY (id1, id2));`},
	{SQL: `INSERT INTO t0 (col_int, col_text, col_real, col_blob) VALUES (3, "jkl", 2.00, X'3231316a6b6c');`},
	{SQL: `INSERT INTO t1 (id1, id2) VALUES (1, 2), (2, 2), (1, 1), (2, 1);`},
}

var testSchema = &sqlite3_schema.Schema{
	TablesVal: []sqlite3_schema.Table{
		{
			NameVal: `t0`,
			ColumnsVal: []sqlite3_schema.Column{
				{NameVal: `col_int`, TypeVal: "INT"},
				{NameVal: `col_text`, TypeVal: "TEXT"},
				{NameVal: `col_real`, TypeVal: "REAL"},
				{NameVal: `col_blob`, TypeVal: "BLOB"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: `t1`,
			ColumnsVal: []sqlite3_schema.Column{
				{NameVal: `id1`, TypeVal: "INT"},
				{NameVal: `id2`, TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
	},
	ReferencesVal: [][]int{{}, {}},
}

var wantOutput = map[string]dml.Rows{
	"t0": []dml.Row{
		map[string]any{
			`col_int`:  sql.NullInt64{Valid: true, Int64: 3},
			`col_text`: sql.NullString{Valid: true, String: `jkl`},
			`col_real`: sql.NullFloat64{Valid: true, Float64: 2.00},
			`col_blob`: []byte(`211jkl`),
		},
	},
	"t1": []dml.Row{
		map[string]any{
			"id1": sql.NullInt64{Valid: true, Int64: 1},
			"id2": sql.NullInt64{Valid: true, Int64: 1}},
		map[string]any{
			"id1": sql.NullInt64{Valid: true, Int64: 1},
			"id2": sql.NullInt64{Valid: true, Int64: 2},
		},
		map[string]any{
			"id1": sql.NullInt64{Valid: true, Int64: 2},
			"id2": sql.NullInt64{Valid: true, Int64: 1},
		},
		map[string]any{
			"id1": sql.NullInt64{Valid: true, Int64: 2},
			"id2": sql.NullInt64{Valid: true, Int64: 2},
		},
	},
}

func getEnvSQLiteTestDirOrSkip(t *testing.T) string {
	t.Helper()

	sqliteTestDir := os.Getenv(test.EnvSQLiteTestDir)
	if sqliteTestDir == "" {
		t.Skipf(`skipped because environment variable %s is not set`, test.EnvSQLiteTestDir)
	}

	return sqliteTestDir
}

func TestDBDeleteFunc_WithSchemaCache(t *testing.T) {
	sqliteTestDir := getEnvSQLiteTestDirOrSkip(t)

	dbPath := fmt.Sprintf(`%s/cli_dbdump_%d.db`, sqliteTestDir, time.Now().UnixNano())
	db, tearDown := test.Setup(t, dbPath, "")
	defer tearDown()

	test.Init(t, db, testInitStmt)

	var schemaReader *bytes.Buffer = bytes.NewBuffer(nil)
	if err := json.NewEncoder(schemaReader).Encode(testSchema); err != nil {
		t.Fatalf(`fail to encode schema to JSON: %v`, err)
	}
	var schemaWriter io.Writer = nil

	input := []string{`t0`, `t1`}

	// sut
	got, err := dbdump.DBDumpFunc(context.Background(), "sqlite3", dbPath, schemaReader, schemaWriter, input)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	want := wantOutput

	checkOutput(t, want, got)
}

func TestDBDeleteFunc_WithoutSchemaCache(t *testing.T) {
	sqliteTestDir := getEnvSQLiteTestDirOrSkip(t)

	dbPath := fmt.Sprintf(`%s/cli_dbdelete_%d.db`, sqliteTestDir, time.Now().UnixNano())
	db, tearDown := test.Setup(t, dbPath, "")
	defer tearDown()

	test.Init(t, db, testInitStmt)

	var schemaReader io.Reader = nil
	var schemaWriter *bytes.Buffer = bytes.NewBuffer(nil)

	input := []string{`t0`, `t1`}

	// sut
	got, err := dbdump.DBDumpFunc(context.Background(), "sqlite3", dbPath, schemaReader, schemaWriter, input)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	want := wantOutput

	checkOutput(t, want, got)

	var gotSchema sqlite3_schema.Schema
	if err := json.NewDecoder(schemaWriter).Decode(&gotSchema); err != nil {
		t.Errorf(`fail to decode schema: %v`, err)
	}
	{
		wantSchema := testSchema
		if len(gotSchema.TablesVal) != len(wantSchema.TablesVal) {
			t.Errorf("len(got.Tables()) != len(want.TablesVal)\n  got  = %v\n  want = %v", got, want)
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
			t.Errorf("references not match\n  got  = %v\n  want = %v", got, want)
		}
	}
}

func checkOutput(t *testing.T, want dbdump.DBDumpOutput, got dbdump.DBDumpOutput) {
	t.Helper()

	if len(got) != len(want) {
		t.Errorf("table count not match\n  got = %v\n  want = %v", len(got), len(want))
	}
	for table, wantRows := range want {
		gotRows, ok := got[table]
		if !ok {
			t.Errorf("table=%s: got value does not have table", table)
		}
		if len(gotRows) != len(wantRows) {
			t.Errorf("row count not match\n  got = %v\n  want = %v", len(gotRows), len(wantRows))
		}
		for i, wantRow := range wantRows {
			gotRow := gotRows[i]
			if len(gotRow) != len(wantRow) {
				t.Errorf("table=%s, rows[%d], column count not match\n  got = %v\n  want = %v", table, i, len(gotRow), len(wantRow))
			}
			for column, wantColumnValue := range wantRow {
				gotColumnValue, ok := gotRow[column]
				if !ok {
					t.Errorf("table=%s, rows[%d], column=%s: got value does not have table", table, i, column)
				}
				if !equals(gotColumnValue, wantColumnValue) {
					t.Errorf("table=%s, rows[%d], column=%s: not equals:\n  gotVal  = %#v\n  wantVal = %#v", table, i, column, gotColumnValue, wantColumnValue)
				}
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
