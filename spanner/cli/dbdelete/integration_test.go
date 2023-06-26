package dbdelete_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/spanner/cli/dbdelete"
	"github.com/Jumpaku/gotaface/spanner/test"
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
	{SQL: `INSERT INTO t3 (id, col1, col2) VALUES (3, 0, 0)`},
	{SQL: `INSERT INTO t4 (id1) VALUES (4)`},
	{SQL: `INSERT INTO t5 (id1, id2) VALUES (4, 5)`},
	{SQL: `INSERT INTO t6 (id1, id2, id3) VALUES (4, 5, 6)`},
	{SQL: `INSERT INTO t7 (id1) VALUES (7)`},
	{SQL: `INSERT INTO t8 (id1, id2) VALUES (7, 8)`},
	{SQL: `INSERT INTO t9 (id1, id2, id3) VALUES (7, 8, 9)`},
}

func TestDBDeleteFunc(t *testing.T) {
	test.SkipIfNoEnv(t)

	env := test.GetEnvSpanner()
	database := fmt.Sprintf(`dbdelete_%d`, time.Now().UnixNano())
	fullDatabase := fmt.Sprintf(`projects/%s/instances/%s/databases/%s`, env.Project, env.Instance, database)

	adminClient, client, tearDown := test.Setup(t, database)
	defer tearDown()

	test.InitDDL(t, adminClient, fullDatabase, testDDLs)
	test.InitDML(t, client, testDMLs)

	input := []string{`t3`, `t2`, `t1`, `t0`, `t6`, `t5`, `t4`, `t9`, `t8`, `t7`}

	// sut
	err := dbdelete.DBDeleteFunc(context.Background(), "spanner", fullDatabase, input)
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
	if r := test.ListRows[struct{}](t, tx, `t4`); len(r) != 0 {
		t.Errorf("t3 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t5`); len(r) != 0 {
		t.Errorf("t5 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t6`); len(r) != 0 {
		t.Errorf("t6 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t7`); len(r) != 0 {
		t.Errorf("t3 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t8`); len(r) != 0 {
		t.Errorf("t8 not deleted")
	}
	if r := test.ListRows[struct{}](t, tx, `t9`); len(r) != 0 {
		t.Errorf("t9 not deleted")
	}
}
