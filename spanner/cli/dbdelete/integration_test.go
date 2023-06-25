package dbdelete_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/spanner/cli/dbdelete"
	"github.com/Jumpaku/gotaface/spanner/ddl/schema"
	"github.com/Jumpaku/gotaface/spanner/test"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/exp/slices"
)

var testDDLs = []string{`
CREATE TABLE t0 (
	id1 INT64,
	id2 INT64,
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
	INTERLEAVE IN PARENT t8`,
}

var testDMLs = []spanner.Statement{
	{SQL: `INSERT INTO t0 (id1, id2) VALUES (0, 0)`},
	{SQL: `INSERT INTO t1 (id) VALUES (0)`},
	{SQL: `INSERT INTO t2 (id) VALUES (0)`},
	{SQL: `INSERT INTO t3 (id) VALUES (3)`},
	{SQL: `INSERT INTO t4 (id1) VALUES (4)`},
	{SQL: `INSERT INTO t5 (id1, id2) VALUES (4, 5)`},
	{SQL: `INSERT INTO t6 (id1, id2, id3) VALUES (4, 5, 6)`},
	{SQL: `INSERT INTO t7 (id1) VALUES (7)`},
	{SQL: `INSERT INTO t8 (id1, id2) VALUES (7, 8)`},
	{SQL: `INSERT INTO t9 (id1, id2, id3) VALUES (7, 8, 9)`},
}

var testSchema = schema.Schema{
	TablesVal: []schema.Table{
		{
			NameVal: "t0",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		{
			NameVal: "t1",
			ColumnsVal: []schema.Column{
				{NameVal: "id", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t2",
			ColumnsVal: []schema.Column{
				{NameVal: "id", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t3",
			ColumnsVal: []schema.Column{
				{NameVal: "id", TypeVal: "INT64"},
				{NameVal: "col1", TypeVal: "INT64"},
				{NameVal: "col2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t4",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t5",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		{
			NameVal: "t6",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
				{NameVal: "id3", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1, 2},
		},
		{
			NameVal: "t7",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0},
		},
		{
			NameVal: "t8",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1},
		},
		{
			NameVal: "t9",
			ColumnsVal: []schema.Column{
				{NameVal: "id1", TypeVal: "INT64"},
				{NameVal: "id2", TypeVal: "INT64"},
				{NameVal: "id3", TypeVal: "INT64"},
			},
			PrimaryKeyVal: []int{0, 1, 2},
		},
	},
	ParentTables:  []*int{nil, nil, nil, nil, nil, nil, nil, nil, ptr(7), ptr(8)},
	ForeignTables: [][]int{{}, {0}, {0}, {1, 2}, {}, {4}, {5}, {}, {}, {}},
}

func TestDBDeleteFunc_WithoutCache(t *testing.T) {
	test.SkipIfNoEnv(t)

	env := test.GetEnvSpanner()
	database := fmt.Sprintf(`dbdelete_%d`, time.Now().UnixNano())
	fullDatabase := fmt.Sprintf(`projects/%s/instances/%s/databases/%s`, env.Project, env.Instance, database)

	adminClient, client, tearDown := test.Setup(t, database)
	defer tearDown()

	test.InitDDL(t, adminClient, fullDatabase, testDDLs)
	test.InitDML(t, client, testDMLs)

	input := []string{`t0`, `t5`, `t8`}
	var reader io.Reader = nil
	writer := bytes.NewBuffer(nil)

	// sut
	err := dbdelete.DBDeleteFunc(context.Background(), "spanner", fullDatabase, reader, writer, input)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	if r := test.ListRows[struct{}](t, tx, `t0`); len(r) != 0 {
		t.Errorf("t0 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t1`); len(r) != 0 {
		t.Errorf("t1 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t2`); len(r) != 0 {
		t.Errorf("t2 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t3`); len(r) != 0 {
		t.Errorf("t3 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t4`); len(r) == 0 {
		t.Errorf("t4 unexpectedly deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t5`); len(r) != 0 {
		t.Errorf("t5 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t6`); len(r) != 0 {
		t.Errorf("t6 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t7`); len(r) == 0 {
		t.Errorf("t4 unexpectedly deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t8`); len(r) != 0 {
		t.Errorf("t8 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t9`); len(r) != 0 {
		t.Errorf("t9 not deleted")
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
	test.SkipIfNoEnv(t)

	env := test.GetEnvSpanner()
	database := fmt.Sprintf(`dbdelete_%d`, time.Now().UnixNano())
	fullDatabase := fmt.Sprintf(`projects/%s/instances/%s/databases/%s`, env.Project, env.Instance, database)

	adminClient, client, tearDown := test.Setup(t, database)
	defer tearDown()

	test.InitDDL(t, adminClient, fullDatabase, testDDLs)
	test.InitDML(t, client, testDMLs)

	input := []string{`t0`, `t5`, `t8`}
	schemaByte, _ := testSchema.MarshalJSON()
	reader := bytes.NewBuffer(schemaByte)
	writer := bytes.NewBuffer(nil)

	// sut
	err := dbdelete.DBDeleteFunc(context.Background(), "spanner", fullDatabase, reader, writer, input)
	if err != nil {
		t.Errorf(`fail to run: %v`, err)
	}

	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	if r := test.ListRows[struct{}](t, tx, `t0`); len(r) != 0 {
		t.Errorf("t0 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t1`); len(r) != 0 {
		t.Errorf("t1 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t2`); len(r) != 0 {
		t.Errorf("t2 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t3`); len(r) != 0 {
		t.Errorf("t3 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t4`); len(r) == 0 {
		t.Errorf("t4 unexpectedly deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t5`); len(r) != 0 {
		t.Errorf("t5 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t6`); len(r) != 0 {
		t.Errorf("t6 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t7`); len(r) == 0 {
		t.Errorf("t4 unexpectedly deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t8`); len(r) != 0 {
		t.Errorf("t8 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t9`); len(r) != 0 {
		t.Errorf("t9 not deleted")
	}

	var gotSchema schema.Schema
	json.Unmarshal(writer.Bytes(), &gotSchema)

	if writer.Bytes() != nil {
		t.Errorf("writer.Bytes() != nil")
	}
}

func ptr[T any](a T) *T {
	return &a
}
