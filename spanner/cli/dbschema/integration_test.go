package dbschema_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	json_schema "github.com/Jumpaku/gotaface/ddl/schema"
	"github.com/Jumpaku/gotaface/spanner/cli/dbschema"
	"github.com/Jumpaku/gotaface/spanner/test"
	"golang.org/x/exp/slices"
)

func TestRunner_Run_Spanner(t *testing.T) {
	test.SkipIfNoEnv(t)

	env := test.GetEnvSpanner()
	database := fmt.Sprintf(`dbschema_%d`, time.Now().UnixNano())
	fullDatabase := fmt.Sprintf(`projects/%s/instances/%s/databases/%s`, env.Project, env.Instance, database)

	adminClient, _, tearDown := test.Setup(t, database)
	defer tearDown()

	test.InitDDL(t, adminClient, fullDatabase, []string{`
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
	INTERLEAVE IN PARENT t8`})

	in := bytes.NewBuffer(nil)
	out := bytes.NewBuffer(nil)

	sut := dbschema.SpannerRunner{DataSource: fullDatabase}

	err := sut.Run(context.Background(), in, out)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}
	var got json_schema.SchemaFormat
	json.Unmarshal(out.Bytes(), &got)

	want := json_schema.SchemaFormat{
		TablesVal: []json_schema.TableFormat{
			{
				NameVal: "t0",
				ColumnsVal: []json_schema.ColumnFormat{
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
			{
				NameVal: "t1",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t2",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t3",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT64"},
					{NameVal: "col1", TypeVal: "INT64"},
					{NameVal: "col2", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t4",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t5",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1},
			},
			{
				NameVal: "t6",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
					{NameVal: "id3", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1, 2},
			},
			{
				NameVal: "t7",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0},
			},
			{
				NameVal: "t8",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1},
			},
			{
				NameVal: "t9",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT64"},
					{NameVal: "id2", TypeVal: "INT64"},
					{NameVal: "id3", TypeVal: "INT64"},
				},
				PrimaryKeyVal: []int{0, 1, 2},
			},
		},
		ReferencesVal: [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}, {}, {7}, {8}},
	}

	for i, want := range want.TablesVal {
		got := got.TablesVal[i]

		equals := got.NameVal == want.NameVal &&
			slices.Equal(got.ColumnsVal, want.ColumnsVal) &&
			slices.Equal(got.PrimaryKeyVal, want.PrimaryKeyVal)

		if !equals {
			t.Errorf("table %d not match\n  got  = %v\n  want = %v", i, got, want)
		}
	}
	if !slices.EqualFunc(got.ReferencesVal, want.ReferencesVal, func(got, want []int) bool { return slices.Equal(got, want) }) {
		t.Errorf("references not match\n  got  = %v\n  want = %v", got, want)
	}
}
