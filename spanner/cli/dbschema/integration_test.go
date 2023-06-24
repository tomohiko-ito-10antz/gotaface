package dbschema_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/spanner/cli/dbschema"
	"github.com/Jumpaku/gotaface/spanner/ddl/schema"
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

	out, err := dbschema.DBSchemaFunc(context.Background(), "spanner", fullDatabase)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}
	b, err := out.MarshalJSON()
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}
	var got schema.SchemaJSON
	json.Unmarshal(b, &got)

	want := schema.SchemaJSON{
		Tables: []schema.TableJSON{
			{
				Name: "t0",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT64"},
					{Name: "id2", Type: "INT64"},
					{Name: "col_integer", Type: "INT64"},
					{Name: "col_string", Type: "STRING(MAX)"},
					{Name: "col_float", Type: "FLOAT64"},
					{Name: "col_bytes", Type: "BYTES(16)"},
					{Name: "col_bool", Type: "BOOL"},
					{Name: "col_date", Type: "DATE"},
					{Name: "col_timestamp", Type: "TIMESTAMP"},
				},
				PrimaryKey: []int{0, 1},
			},
			{
				Name: "t1",
				Columns: []schema.ColumnJSON{
					{Name: "id", Type: "INT64"},
				},
				PrimaryKey: []int{0},
			},
			{
				Name: "t2",
				Columns: []schema.ColumnJSON{
					{Name: "id", Type: "INT64"},
				},
				PrimaryKey: []int{0},
			},
			{
				Name: "t3",
				Columns: []schema.ColumnJSON{
					{Name: "id", Type: "INT64"},
					{Name: "col1", Type: "INT64"},
					{Name: "col2", Type: "INT64"},
				},
				PrimaryKey: []int{0},
			},
			{
				Name: "t4",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT64"},
				},
				PrimaryKey: []int{0},
			},
			{
				Name: "t5",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT64"},
					{Name: "id2", Type: "INT64"},
				},
				PrimaryKey: []int{0, 1},
			},
			{
				Name: "t6",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT64"},
					{Name: "id2", Type: "INT64"},
					{Name: "id3", Type: "INT64"},
				},
				PrimaryKey: []int{0, 1, 2},
			},
			{
				Name: "t7",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT64"},
				},
				PrimaryKey: []int{0},
			},
			{
				Name: "t8",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT64"},
					{Name: "id2", Type: "INT64"},
				},
				PrimaryKey: []int{0, 1},
			},
			{
				Name: "t9",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT64"},
					{Name: "id2", Type: "INT64"},
					{Name: "id3", Type: "INT64"},
				},
				PrimaryKey: []int{0, 1, 2},
			},
		},
		References: [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}, {}, {7}, {8}},
	}

	if len(got.Tables) != len(want.Tables) {
		t.Errorf("len(got.Tables()) != len(want.TablesVal)\n  got  = %v\n  want = %v", got, want)
	}
	for i, want := range want.Tables {
		got := got.Tables[i]

		equals := got.Name == want.Name &&
			slices.Equal(got.Columns, want.Columns) &&
			slices.Equal(got.PrimaryKey, want.PrimaryKey)

		if !equals {
			t.Errorf("table %d not match\n  got  = %v\n  want = %v", i, got, want)
		}
	}
	if !slices.EqualFunc(got.References, want.References, func(got, want []int) bool { return slices.Equal(got, want) }) {
		t.Errorf("references not match\n  got  = %v\n  want = %v", got, want)
	}
}
