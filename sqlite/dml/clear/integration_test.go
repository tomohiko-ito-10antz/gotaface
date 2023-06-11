package clear_test

import (
	"context"
	"os"
	"testing"

	"github.com/Jumpaku/gotaface/sqlite/dml/clear"
	"github.com/Jumpaku/gotaface/sqlite/test"

	_ "github.com/mattn/go-sqlite3"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestClearer_Clear(t *testing.T) {

	db, tearDown, err := test.Setup()
	if err != nil {
		t.Fatalf(`fail to setup DB for test: %v`, err)
	}
	defer tearDown()

	ctx := context.Background()
	_, err = db.ExecContext(ctx, `
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
`)
	if err != nil {
		tearDown()
		t.Fatalf("fail to fetch tables: %v", err)
	}

	sut := clear.NewClearer(db)
	err = sut.Clear(ctx, `t`)
	if err != nil {
		tearDown()
		t.Errorf("fail to clear table: %v", err)
	}

	rows, err := db.QueryContext(ctx, `SELECT * FROM t`)
	if err != nil {
		tearDown()
		t.Fatalf("fail to select all: %v", err)
	}
	if rows.Next() {
		tearDown()
		t.Errorf("rows are remaining")
	}
}
