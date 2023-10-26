package dbdump_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/old/dml"
	"github.com/Jumpaku/gotaface/old/spanner/cli/dbdump"
	spanner_schema "github.com/Jumpaku/gotaface/old/spanner/ddl/schema"
	"github.com/Jumpaku/gotaface/old/spanner/test"
	"golang.org/x/exp/slices"
)

var testDDLs = []string{`
CREATE TABLE t0 (
	id1 INT64,
	id2 INT64,
) PRIMARY KEY (id1, id2)
`, `
CREATE TABLE t1 (
	col_integer   INT64,
	col_string    STRING(MAX),
	col_float     FLOAT64,
	col_bytes     BYTES(16),
	col_bool      BOOL,
	col_timestamp TIMESTAMP,
	col_date      DATE,
	col_json      JSON,
	col_array    ARRAY<INT64>,
) PRIMARY KEY (col_integer)
`,
}

var testDMLs = []spanner.Statement{
	{SQL: `INSERT INTO t0 (id1, id2) VALUES (1, 2), (2, 2), (1, 1), (2, 1)`},
	{SQL: `INSERT INTO t1 (col_integer, col_string, col_float, col_bytes, col_bool, col_timestamp, col_date, col_json, col_array) VALUES (1, 'abc', 1.25, b'1234abcd', TRUE,  '2023-06-11T01:23:45Z', '2023-06-11', JSON'{"a":1, "b":"x", "c":null, "d":[{}, []], "e":{"x":{}, "y":[]}}', [1, 2, 3])`},
}

var testSchema = &spanner_schema.Schema{
	TablesVal: []spanner_schema.Table{
		{
			NameVal: `t0`,
			ColumnsVal: []spanner_schema.Column{
				{NameVal: `id1`, TypeVal: "INT64"},
				{NameVal: `id2`, TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		{
			NameVal: `t1`,
			ColumnsVal: []spanner_schema.Column{
				{NameVal: `col_integer`, TypeVal: "INT64"},
				{NameVal: `col_string`, TypeVal: "STRING(MAX)"},
				{NameVal: `col_float`, TypeVal: "FLOAT64"},
				{NameVal: `col_bytes`, TypeVal: "BYTES(16)"},
				{NameVal: `col_bool`, TypeVal: "BOOL"},
				{NameVal: `col_timestamp`, TypeVal: "TIMESTAMP"},
				{NameVal: `col_date`, TypeVal: "DATE"},
				{NameVal: `col_json`, TypeVal: "JSON"},
				{NameVal: `col_array`, TypeVal: "ARRAY<INT64>"},
			},
			PrimaryKeyVal: []int{0},
		},
	},
	ParentTables:  []*int{nil, nil},
	ForeignTables: [][]int{{}, {}},
}

var wantTime, _ = time.Parse(time.RFC3339, `2023-06-11T01:23:45Z`)
var wantJSON = map[string]any{"a": 1, "b": "x", "c": nil, "d": []any{map[string]any{}, []any{}}, "e": map[string]any{"x": map[string]any{}, "y": []any{}}}

var wantOutput = map[string]dml.Rows{
	"t0": []dml.Row{
		map[string]any{
			"id1": spanner.NullInt64{Valid: true, Int64: 1},
			"id2": spanner.NullInt64{Valid: true, Int64: 1}},
		map[string]any{
			"id1": spanner.NullInt64{Valid: true, Int64: 1},
			"id2": spanner.NullInt64{Valid: true, Int64: 2},
		},
		map[string]any{
			"id1": spanner.NullInt64{Valid: true, Int64: 2},
			"id2": spanner.NullInt64{Valid: true, Int64: 1},
		},
		map[string]any{
			"id1": spanner.NullInt64{Valid: true, Int64: 2},
			"id2": spanner.NullInt64{Valid: true, Int64: 2},
		},
	},
	"t1": []dml.Row{
		map[string]any{
			`col_integer`:   spanner.NullInt64{Valid: true, Int64: 1},
			`col_string`:    spanner.NullString{Valid: true, StringVal: `abc`},
			`col_float`:     spanner.NullFloat64{Valid: true, Float64: 1.25},
			`col_bytes`:     []byte(`1234abcd`),
			`col_bool`:      spanner.NullBool{Valid: true, Bool: true},
			`col_timestamp`: spanner.NullTime{Valid: true, Time: wantTime},
			`col_date`:      spanner.NullDate{Valid: true, Date: civil.DateOf(wantTime)},
			`col_json`:      spanner.NullJSON{Valid: true, Value: wantJSON},
			`col_array`:     []spanner.NullInt64{{Valid: true, Int64: 1}, {Valid: true, Int64: 2}, {Valid: true, Int64: 3}},
		},
	},
}

func equals(got any, want any) bool {
	switch want.(type) {
	default:
		return got == want
	case []byte:
		return slices.Equal(got.([]byte), want.([]byte))
	case spanner.NullJSON:
		got, _ := got.(spanner.NullJSON).MarshalJSON()
		want, _ := want.(spanner.NullJSON).MarshalJSON()
		return string(got) == string(want)
	case []spanner.NullInt64:
		return slices.Equal(got.([]spanner.NullInt64), want.([]spanner.NullInt64))
	}
}

func TestDBDeleteFunc_WithSchemaCache(t *testing.T) {
	test.SkipIfNoEnv(t)

	env := test.GetEnvSpanner()
	database := fmt.Sprintf(`dbdump_%d`, time.Now().UnixNano())
	fullDatabase := fmt.Sprintf(`projects/%s/instances/%s/databases/%s`, env.Project, env.Instance, database)

	adminClient, client, tearDown := test.Setup(t, database)
	defer tearDown()

	test.InitDDL(t, adminClient, fullDatabase, testDDLs)
	test.InitDML(t, client, testDMLs)

	var schemaReader *bytes.Buffer = bytes.NewBuffer(nil)
	if err := json.NewEncoder(schemaReader).Encode(testSchema); err != nil {
		t.Fatalf(`fail to encode schema to JSON: %v`, err)
	}
	var schemaWriter io.Writer = nil

	input := []string{`t0`, `t1`}

	// sut
	got, err := dbdump.DBDumpFunc(context.Background(), "spanner", fullDatabase, schemaReader, schemaWriter, input)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	want := wantOutput

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

func TestDBDeleteFunc_WithoutSchemaCache(t *testing.T) {
	test.SkipIfNoEnv(t)

	env := test.GetEnvSpanner()
	database := fmt.Sprintf(`dbdump_%d`, time.Now().UnixNano())
	fullDatabase := fmt.Sprintf(`projects/%s/instances/%s/databases/%s`, env.Project, env.Instance, database)

	adminClient, client, tearDown := test.Setup(t, database)
	defer tearDown()

	test.InitDDL(t, adminClient, fullDatabase, testDDLs)
	test.InitDML(t, client, testDMLs)

	var schemaReader io.Reader = nil
	var schemaWriter *bytes.Buffer = bytes.NewBuffer(nil)

	input := []string{`t0`, `t1`}

	// sut
	got, err := dbdump.DBDumpFunc(context.Background(), "spanner", fullDatabase, schemaReader, schemaWriter, input)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	want := wantOutput

	if len(got) != len(wantOutput) {
		t.Errorf("table count not match\n  got = %v\n  want = %v", len(got), len(want))
	}
	for table, wantRows := range wantOutput {
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

	var gotSchema spanner_schema.Schema
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
