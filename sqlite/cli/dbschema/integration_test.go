package dbschema_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	json_schema "github.com/Jumpaku/gotaface/ddl/schema"
	"github.com/Jumpaku/gotaface/sqlite/cli/dbschema"
	"github.com/Jumpaku/gotaface/sqlite/test"
	"golang.org/x/exp/slices"
)

func TestRunner_Run_Sqlite(t *testing.T) {
	sqliteTestDir := os.Getenv(test.EnvSqliteTestDir)
	if sqliteTestDir == "" {
		t.Skipf(`skipped because environment variable %s is not set`, test.EnvSqliteTestDir)
	}

	dataSource := fmt.Sprintf(`%s/cli_dbschema_%d.db`, sqliteTestDir, time.Now().UnixNano())
	db, tearDown := test.Setup(t, dataSource)
	defer tearDown()

	test.Init(t, db, []test.Statement{{SQL: `
CREATE TABLE t0 (
	id1 INT,
	id2 INT,
	col_integer INTEGER,
	col_text TEXT,
	col_real REAL,
	col_blob BLOB,
	PRIMARY KEY (id1, id2));
	
CREATE TABLE t1 (
	id INT,
	PRIMARY KEY (id),
	FOREIGN KEY (id) REFERENCES t0 (id1));
	
CREATE TABLE t2 (
	id INT,
	PRIMARY KEY (id),
	FOREIGN KEY (id) REFERENCES t0 (id2));
	
CREATE TABLE t3 (
	id INT,
	col1 INT,
	col2 INT,
	PRIMARY KEY (id),
	FOREIGN KEY (col1) REFERENCES t1 (id),
	FOREIGN KEY (col2) REFERENCES t2 (id));
	
CREATE TABLE t4 (
	id INT,
	PRIMARY KEY (id));
	
CREATE TABLE t5 (
	id1 INT,
	id2 INT,
	PRIMARY KEY (id1, id2),
	FOREIGN KEY (id1) REFERENCES t4 (id));

CREATE TABLE t6 (
	id1 INT,
	id2 INT,
	id3 INT,
	PRIMARY KEY (id1, id2, id3),
	FOREIGN KEY (id1, id2) REFERENCES t5 (id1, id2));
`}})

	in := bytes.NewBuffer(nil)
	out := bytes.NewBuffer(nil)

	sut := dbschema.SqliteRunner{DataSource: dataSource}

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
					{NameVal: "id1", TypeVal: "INT"},
					{NameVal: "id2", TypeVal: "INT"},
					{NameVal: "col_integer", TypeVal: "INTEGER"},
					{NameVal: "col_text", TypeVal: "TEXT"},
					{NameVal: "col_real", TypeVal: "REAL"},
					{NameVal: "col_blob", TypeVal: "BLOB"},
				},
				PrimaryKeyVal: []int{0, 1},
			}, {
				NameVal: "t1",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT"},
				},
				PrimaryKeyVal: []int{0},
			}, {
				NameVal: "t2",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT"},
				},
				PrimaryKeyVal: []int{0},
			}, {
				NameVal: "t3",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT"},
					{NameVal: "col1", TypeVal: "INT"},
					{NameVal: "col2", TypeVal: "INT"},
				},
				PrimaryKeyVal: []int{0},
			}, {
				NameVal: "t4",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id", TypeVal: "INT"},
				},
				PrimaryKeyVal: []int{0},
			}, {
				NameVal: "t5",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT"},
					{NameVal: "id2", TypeVal: "INT"},
				},
				PrimaryKeyVal: []int{0, 1},
			}, {
				NameVal: "t6",
				ColumnsVal: []json_schema.ColumnFormat{
					{NameVal: "id1", TypeVal: "INT"},
					{NameVal: "id2", TypeVal: "INT"},
					{NameVal: "id3", TypeVal: "INT"},
				},
				PrimaryKeyVal: []int{0, 1, 2},
			},
		},
		ReferencesVal: [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}},
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
