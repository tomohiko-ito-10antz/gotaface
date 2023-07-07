package dbinsert_test

/*
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml"
	gotaface_spanner "github.com/Jumpaku/gotaface/spanner"
	"github.com/Jumpaku/gotaface/spanner/cli/dbinsert"
	spanner_schema "github.com/Jumpaku/gotaface/spanner/ddl/schema"
	"github.com/Jumpaku/gotaface/spanner/test"
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

var wantJSON = map[string]any{"a": 1, "b": "x", "c": nil, "d": []any{map[string]any{}, []any{}}, "e": map[string]any{"x": map[string]any{}, "y": []any{}}}

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
				`col_integer`:   1,
				`col_string`:    `abc`,
				`col_float`:     1.25,
				`col_bytes`:     []byte(`1234abcd`),
				`col_bool`:      true,
				`col_timestamp`: "2023-07-03T00:23:32Z",
				`col_date`:      "2023-07-03",
				`col_json`:      wantJSON,
				`col_array`:     []any{1, 2, 3},
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
	case spanner.NullJSON:
		got, _ := got.(spanner.NullJSON).MarshalJSON()
		want, _ := want.(spanner.NullJSON).MarshalJSON()
		return string(got) == string(want)
	case []spanner.NullInt64:
		return slices.Equal(got.([]spanner.NullInt64), want.([]spanner.NullInt64))
	}
}

func TestDBInsertFunc_WithSchemaCache(t *testing.T) {
	test.SkipIfNoEnv(t)

	env := test.GetEnvSpanner()
	database := fmt.Sprintf(`dbinsert_%d`, time.Now().UnixNano())
	fullDatabase := fmt.Sprintf(`projects/%s/instances/%s/databases/%s`, env.Project, env.Instance, database)

	adminClient, client, tearDown := test.Setup(t, database)
	defer tearDown()

	test.InitDDL(t, adminClient, fullDatabase, testDDLs)

	var schemaReader *bytes.Buffer = bytes.NewBuffer(nil)
	if err := json.NewEncoder(schemaReader).Encode(testSchema); err != nil {
		t.Fatalf(`fail to encode schema to JSON: %v`, err)
	}
	var schemaWriter io.Writer = nil

	// sut
	err := dbinsert.DBInsertFunc(context.Background(), "spanner", fullDatabase, schemaReader, schemaWriter, testInput)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	for _, want := range testInput[0].Rows() {
		key := map[string]any{"id1": want["id1"], "id2": want["id2"]}
		got := test.FindRow[struct {
			Id1 int64
			Id2 int64
		}](t, client.Single(), `t0`, key)
		if got == nil {
			t.Errorf("row not found\n  got  = %v\n  want = %v", got, want)
		}
	}
	for _, want := range testInput[1].Rows() {
		var (
			Col_integer, _   = gotaface_spanner.ToDBValue("INT64", want["col_integer"])
			Col_string, _    = gotaface_spanner.ToDBValue("STRING(MAX)", want["col_string"])
			Col_float, _     = gotaface_spanner.ToDBValue("FLOAT64", want["col_float"])
			Col_bytes, _     = gotaface_spanner.ToDBValue("BYTES(256)", want["col_bytes"])
			Col_bool, _      = gotaface_spanner.ToDBValue("BOOL", want["col_bool"])
			Col_timestamp, _ = gotaface_spanner.ToDBValue("TIMESTAMP", want["col_timestamp"])
			Col_date, _      = gotaface_spanner.ToDBValue("DATE", want["col_date"])
			Col_json, _      = gotaface_spanner.ToDBValue("JSON", want["col_json"])
			Col_array, _     = gotaface_spanner.ToDBValue("ARRAY<INT64>", want["col_array"])
		)
		got := test.FindRow[struct {
			Col_integer   spanner.NullInt64
			Col_string    spanner.NullString
			Col_float     spanner.NullFloat64
			Col_bytes     []byte
			Col_bool      spanner.NullBool
			Col_timestamp spanner.NullTime
			Col_date      spanner.NullDate
			Col_json      spanner.NullJSON
			Col_array     []spanner.NullInt64
		}](t, client.Single(), `t1`, map[string]any{"Col_integer": Col_integer})
		if got == nil {
			t.Errorf("row not found\n  got = %v\n  want = %v", got, want)
		}
		eq := equals(got.Col_integer, Col_integer) &&
			equals(got.Col_string, Col_string) &&
			equals(got.Col_float, Col_float) &&
			equals(got.Col_bytes, Col_bytes) &&
			equals(got.Col_bool, Col_bool) &&
			equals(got.Col_timestamp, Col_timestamp) &&
			equals(got.Col_date, Col_date) &&
			equals(got.Col_json, Col_json) &&
			equals(got.Col_array, Col_array)
		if !eq {
			t.Errorf("row not found\n  got = %v\n  want = %v", got, want)
		}
	}
}

func TestDBInsertFunc_WithoutSchemaCache(t *testing.T) {
	test.SkipIfNoEnv(t)

	env := test.GetEnvSpanner()
	database := fmt.Sprintf(`dbinsert_%d`, time.Now().UnixNano())
	fullDatabase := fmt.Sprintf(`projects/%s/instances/%s/databases/%s`, env.Project, env.Instance, database)

	adminClient, client, tearDown := test.Setup(t, database)
	defer tearDown()

	test.InitDDL(t, adminClient, fullDatabase, testDDLs)

	var schemaReader io.Reader = nil
	var schemaWriter *bytes.Buffer = bytes.NewBuffer(nil)

	// sut
	err := dbinsert.DBInsertFunc(context.Background(), "spanner", fullDatabase, schemaReader, schemaWriter, testInput)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	for _, want := range testInput[0].Rows() {
		key := map[string]any{"id1": want["id1"], "id2": want["id2"]}
		got := test.FindRow[struct {
			Id1 int64
			Id2 int64
		}](t, client.Single(), `t0`, key)
		if got == nil {
			t.Errorf("row not found\n  got  = %v\n  want = %v", got, want)
		}
	}
	for _, want := range testInput[1].Rows() {
		var (
			Col_integer, _   = gotaface_spanner.ToDBValue("INT64", want["col_integer"])
			Col_string, _    = gotaface_spanner.ToDBValue("STRING(MAX)", want["col_string"])
			Col_float, _     = gotaface_spanner.ToDBValue("FLOAT64", want["col_float"])
			Col_bytes, _     = gotaface_spanner.ToDBValue("BYTES(256)", want["col_bytes"])
			Col_bool, _      = gotaface_spanner.ToDBValue("BOOL", want["col_bool"])
			Col_timestamp, _ = gotaface_spanner.ToDBValue("TIMESTAMP", want["col_timestamp"])
			Col_date, _      = gotaface_spanner.ToDBValue("DATE", want["col_date"])
			Col_json, _      = gotaface_spanner.ToDBValue("JSON", want["col_json"])
			Col_array, _     = gotaface_spanner.ToDBValue("ARRAY<INT64>", want["col_array"])
		)
		got := test.FindRow[struct {
			Col_integer   spanner.NullInt64
			Col_string    spanner.NullString
			Col_float     spanner.NullFloat64
			Col_bytes     []byte
			Col_bool      spanner.NullBool
			Col_timestamp spanner.NullTime
			Col_date      spanner.NullDate
			Col_json      spanner.NullJSON
			Col_array     []spanner.NullInt64
		}](t, client.Single(), `t1`, map[string]any{"Col_integer": Col_integer})
		if got == nil {
			t.Errorf("row not found\n  got = %v\n  want = %v", got, want)
		}
		eq := equals(got.Col_integer, Col_integer) &&
			equals(got.Col_string, Col_string) &&
			equals(got.Col_float, Col_float) &&
			equals(got.Col_bytes, Col_bytes) &&
			equals(got.Col_bool, Col_bool) &&
			equals(got.Col_timestamp, Col_timestamp) &&
			equals(got.Col_date, Col_date) &&
			equals(got.Col_json, Col_json) &&
			equals(got.Col_array, Col_array)
		if !eq {
			t.Errorf("row not found\n  got = %v\n  want = %v", got, want)
		}
	}

	var gotSchema spanner_schema.Schema
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
*/
