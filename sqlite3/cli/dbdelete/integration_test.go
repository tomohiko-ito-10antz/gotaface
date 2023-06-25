package dbdelete_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/sqlite3/cli/dbdelete"
	"github.com/Jumpaku/gotaface/sqlite3/ddl/schema"
	"github.com/Jumpaku/gotaface/sqlite3/test"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/exp/slices"
)

var testInitStmt = []test.Statement{
	{SQL: `PRAGMA foreign_keys = 1`},
	{SQL: `
CREATE TABLE t0 (
	id1 INT UNIQUE,
	id2 INT UNIQUE,
	PRIMARY KEY (id1, id2));`},
	{SQL: `
CREATE TABLE t1 (
	id INT,
	PRIMARY KEY (id),
	FOREIGN KEY (id) REFERENCES t0 (id1));`},
	{SQL: `
CREATE TABLE t2 (
	id INT,
	PRIMARY KEY (id),
	FOREIGN KEY (id) REFERENCES t0 (id2));`},
	{SQL: `
CREATE TABLE t3 (
	id INT,
	col1 INT,
	col2 INT,
	PRIMARY KEY (id),
	FOREIGN KEY (col1) REFERENCES t1 (id),
	FOREIGN KEY (col2) REFERENCES t2 (id));`},
	{SQL: `
CREATE TABLE t4 (
	id1 INT,
	PRIMARY KEY (id1));`},
	{SQL: `
CREATE TABLE t5 (
	id1 INT,
	id2 INT,
	PRIMARY KEY (id1, id2),
	FOREIGN KEY (id1) REFERENCES t4 (id1));`},
	{SQL: `
CREATE TABLE t6 (
	id1 INT,
	id2 INT,
	id3 INT,
	PRIMARY KEY (id1, id2, id3),
	FOREIGN KEY (id1, id2) REFERENCES t5 (id1, id2))`},
	{SQL: `INSERT INTO t0 (id1, id2) VALUES (0, 0)`},
	{SQL: `INSERT INTO t1 (id) VALUES (0)`},
	{SQL: `INSERT INTO t2 (id) VALUES (0)`},
	{SQL: `INSERT INTO t3 (id, col1, col2) VALUES (3, 0, 0)`},
	{SQL: `INSERT INTO t4 (id1) VALUES (4)`},
	{SQL: `INSERT INTO t5 (id1, id2) VALUES (4, 5)`},
	{SQL: `INSERT INTO t6 (id1, id2, id3) VALUES (4, 5, 6)`},
}

var testSchema = schema.Schema{
	TablesVal: []schema.Table{
		{
			NameVal: "t0",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT"},
				{NameVal: "id2", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		{
			NameVal: "t1",
			ColumnsVal: []schema.Column{
				{NameVal: "id", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t2",
			ColumnsVal: []schema.Column{
				{NameVal: "id", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t3",
			ColumnsVal: []schema.Column{
				{NameVal: "id", TypeVal: "INT"},
				{NameVal: "col1", TypeVal: "INT"},
				{NameVal: "col2", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t4",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t5",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT"},
				{NameVal: "id2", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		{
			NameVal: "t6",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT"},
				{NameVal: "id2", TypeVal: "INT"},
				{NameVal: "id3", TypeVal: "INT"},
			},
			PrimaryKeyVal: []int{0, 1, 2},
		},
	},
	ReferencesVal: [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}},
}

func TestDBDeleteFunc_WithoutCache(t *testing.T) {
	sqliteTestDir := os.Getenv(test.EnvSqliteTestDir)
	if sqliteTestDir == "" {
		t.Skipf(`skipped because environment variable %s is not set`, test.EnvSqliteTestDir)
	}

	dataSource := fmt.Sprintf(`%s/cli_dbdelete_%d.db`, sqliteTestDir, time.Now().UnixNano())
	db, tearDown := test.Setup(t, dataSource, `_foreign_keys=1`)
	defer tearDown()

	test.Init(t, db, testInitStmt)

	input := []string{`t0`, `t5`}
	var reader io.Reader = nil
	writer := bytes.NewBuffer(nil)

	// sut
	err := dbdelete.DBDeleteFunc(context.Background(), "sqlite3", dataSource, reader, writer, input)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	if r := test.ListRows[struct{}](t, db, `t0`); len(r) != 0 {
		t.Errorf("t0 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t1`); len(r) != 0 {
		t.Errorf("t1 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t2`); len(r) != 0 {
		t.Errorf("t2 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t3`); len(r) != 0 {
		t.Errorf("t3 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t4`); len(r) == 0 {
		t.Errorf("t4 unexpectedly deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t5`); len(r) != 0 {
		t.Errorf("t5 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t6`); len(r) != 0 {
		t.Errorf("t6 not deleted")
	}

	var gotSchema schema.Schema
	spew.Dump(writer.String())
	json.Unmarshal(writer.Bytes(), &gotSchema)

	if len(gotSchema.Tables()) != len(testSchema.Tables()) {
		t.Errorf("len(got.Tables()) != len(want.Tables())\n  got  = %v\n  want = %v", len(gotSchema.Tables()), len(testSchema.Tables()))
	}
	for i, want := range testSchema.Tables() {
		got := gotSchema.Tables()[i]

		equals := got.Name() == want.Name() &&
			slices.Equal(got.Columns(), want.Columns()) &&
			slices.Equal(got.PrimaryKey(), want.PrimaryKey())

		if !equals {
			t.Errorf("table %d not match\n  got  = %v\n  want = %v", i, got, want)
		}
	}
	if !slices.EqualFunc(gotSchema.References(), testSchema.References(), func(got, want []int) bool { return slices.Equal(got, want) }) {
		t.Errorf("references not match\n  got  = %v\n  want = %v", gotSchema.References(), testSchema.References())
	}
}

func TestDBDeleteFunc_WithCache(t *testing.T) {
	sqliteTestDir := os.Getenv(test.EnvSqliteTestDir)
	if sqliteTestDir == "" {
		t.Skipf(`skipped because environment variable %s is not set`, test.EnvSqliteTestDir)
	}

	dbPath := fmt.Sprintf(`%s/cli_dbdelete_%d.db`, sqliteTestDir, time.Now().UnixNano())
	db, tearDown := test.Setup(t, dbPath, `_foreign_keys=1`)
	defer tearDown()

	test.Init(t, db, testInitStmt)

	input := []string{`t0`, `t5`}
	schemaByte, _ := testSchema.MarshalJSON()
	reader := bytes.NewBuffer(schemaByte)
	writer := bytes.NewBuffer(nil)

	// sut
	err := dbdelete.DBDeleteFunc(context.Background(), "sqlite3", dbPath, reader, writer, input)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	if r := test.ListRows[struct{}](t, db, `t0`); len(r) != 0 {
		t.Errorf("t0 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t1`); len(r) != 0 {
		t.Errorf("t1 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t2`); len(r) != 0 {
		t.Errorf("t2 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t3`); len(r) != 0 {
		t.Errorf("t3 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t4`); len(r) == 0 {
		t.Errorf("t4 unexpectedly deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t5`); len(r) != 0 {
		t.Errorf("t5 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t6`); len(r) != 0 {
		t.Errorf("t6 not deleted")
	}

	if writer.Bytes() != nil {
		t.Errorf("writer.Bytes() != nil")
	}
}
