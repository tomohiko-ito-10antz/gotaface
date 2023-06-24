package delete_test

import (
	"context"
	"os"
	"testing"

	"github.com/Jumpaku/gotaface/sqlite3/dml/delete"
	"github.com/Jumpaku/gotaface/sqlite3/test"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDeleter_Clear(t *testing.T) {

	db, tearDown := test.Setup(t, "")
	defer tearDown()

	ctx := context.Background()
	test.Init(t, db, []test.Statement{{
		SQL: `
CREATE TABLE t (
	id2 INT,
	id1 INT,
	col_integer INTEGER,
	col_text TEXT,
	col_real REAL,
	col_blob BLOB,
	PRIMARY KEY (id1, id2));

INSERT INTO t (id1, id2, col_integer, col_text, col_real, col_blob)
VALUES
	(1, 2, 4, "abc", 1.25, X'313234616263'),
	(2, 2, 3, "def", 1.50, X'323233646566'),
	(1, 1, 2, "ghi", 1.75, X'313132676869'),
	(2, 1, 1, "jkl", 2.00, X'3231316a6b6c');
`,
	}})

	sut := delete.NewDeleter(db)
	err := sut.Delete(ctx, `t`)
	if err != nil {
		t.Errorf("fail to delete table: %v", err)
	}

	rows := test.ListRows[struct{}](t, db, `t`)
	if len(rows) > 0 {
		t.Errorf("rows are remaining: %v", err)
	}
}
