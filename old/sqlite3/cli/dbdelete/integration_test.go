package dbdelete_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/old/sqlite3/cli/dbdelete"
	"github.com/Jumpaku/gotaface/old/sqlite3/test"
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

func TestDBDeleteFunc_ForeignKey(t *testing.T) {
	sqliteTestDir := os.Getenv(test.EnvSQLiteTestDir)
	if sqliteTestDir == "" {
		t.Skipf(`skipped because environment variable %s is not set`, test.EnvSQLiteTestDir)
	}

	dbPath := fmt.Sprintf(`%s/cli_dbdelete_%d.db`, sqliteTestDir, time.Now().UnixNano())
	db, tearDown := test.Setup(t, dbPath, `_foreign_keys=1`)
	defer tearDown()

	test.Init(t, db, testInitStmt)

	input := []string{`t3`, `t2`, `t1`, `t0`, `t6`, `t5`, `t4`}

	// sut
	err := dbdelete.DBDeleteFunc(context.Background(), "sqlite3", dbPath, input)
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
	if r := test.ListRows[struct{}](t, db, `t4`); len(r) != 0 {
		t.Errorf("t3 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t5`); len(r) != 0 {
		t.Errorf("t5 not deleted")
	}
	if r := test.ListRows[struct{}](t, db, `t6`); len(r) != 0 {
		t.Errorf("t6 not deleted")
	}
}
