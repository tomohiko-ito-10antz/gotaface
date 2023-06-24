package dbschema_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/sqlite/cli/dbschema"
	"github.com/Jumpaku/gotaface/sqlite/ddl/schema"
	"github.com/Jumpaku/gotaface/sqlite/test"
	"golang.org/x/exp/slices"
)

func TestRunner_Run_SQLite(t *testing.T) {
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

	sut := dbschema.SQLiteRunner{DataSource: dataSource}

	err := sut.Run(context.Background(), in, out)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}
	var got schema.SchemaJSON
	json.Unmarshal(out.Bytes(), &got)

	want := schema.SchemaJSON{
		Tables: []schema.TableJSON{
			{
				Name: "t0",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT"},
					{Name: "id2", Type: "INT"},
					{Name: "col_integer", Type: "INTEGER"},
					{Name: "col_text", Type: "TEXT"},
					{Name: "col_real", Type: "REAL"},
					{Name: "col_blob", Type: "BLOB"},
				},
				PrimaryKey: []int{0, 1},
			}, {
				Name: "t1",
				Columns: []schema.ColumnJSON{
					{Name: "id", Type: "INT"},
				},
				PrimaryKey: []int{0},
			}, {
				Name: "t2",
				Columns: []schema.ColumnJSON{
					{Name: "id", Type: "INT"},
				},
				PrimaryKey: []int{0},
			}, {
				Name: "t3",
				Columns: []schema.ColumnJSON{
					{Name: "id", Type: "INT"},
					{Name: "col1", Type: "INT"},
					{Name: "col2", Type: "INT"},
				},
				PrimaryKey: []int{0},
			}, {
				Name: "t4",
				Columns: []schema.ColumnJSON{
					{Name: "id", Type: "INT"},
				},
				PrimaryKey: []int{0},
			}, {
				Name: "t5",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT"},
					{Name: "id2", Type: "INT"},
				},
				PrimaryKey: []int{0, 1},
			}, {
				Name: "t6",
				Columns: []schema.ColumnJSON{
					{Name: "id1", Type: "INT"},
					{Name: "id2", Type: "INT"},
					{Name: "id3", Type: "INT"},
				},
				PrimaryKey: []int{0, 1, 2},
			},
		},
		References: [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}},
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
