package schema_test

import (
	"context"
	"database/sql"
	"fmt"
	schema2 "github.com/Jumpaku/gotaface/schema"
	"github.com/Jumpaku/gotaface/sqlite/schema"
	_ "github.com/mattn/go-sqlite3"
	"github.com/oklog/ulid/v2"
	"golang.org/x/exp/slices"
	"os"
	"path/filepath"
	"testing"
)

var db *sql.DB

func TestMain(m *testing.M) {
	initialize()
	os.Exit(m.Run())
}

const envTestSQLiteSchemaDBDir = "GOTAFACE_TEST_SQLITE_SCHEMA"

var (
	skipTest  bool
	testDBDir string
)

func initialize() {
	if os.Getenv(envTestSQLiteSchemaDBDir) == "" {
		skipTest = true
		return
	}

	testDBDir = os.Getenv(envTestSQLiteSchemaDBDir)
}

func newDatabaseName() string {
	return fmt.Sprintf(`gotaface_test_sqlite_schema_%s.db`, ulid.Make().String())
}

func setup(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	if skipTest {
		t.Skip("skip test")
	}
	dataSource := filepath.Join(testDBDir, newDatabaseName())
	os.Remove(dataSource)
	db, err := sql.Open("sqlite3", dataSource)
	if err != nil {
		t.Fatal(err)
	}

	tearDown := func() {
		db.Close()
		os.Remove(dataSource)
	}

	return db, tearDown
}

func TestLister_List(t *testing.T) {
	db, tearDown := setup(t)
	defer tearDown()

	_, err := db.Exec(`
CREATE TABLE t1 (
  id1 INT,
  id2 INT,
  col_integer INTEGER,
  col_text TEXT,
  col_real REAL,
  col_blob BLOB,
  PRIMARY KEY (id1, id2));
  
CREATE TABLE t2 (
  id INT,
  PRIMARY KEY (id),
  FOREIGN KEY (id) REFERENCES t1 (id1));
  
CREATE TABLE t3 (
  id INT,
  PRIMARY KEY (id),
  FOREIGN KEY (id) REFERENCES t1 (id2));
  
CREATE TABLE t4 (
  id INT,
  col1 INT,
  col2 INT,
  PRIMARY KEY (id),
  FOREIGN KEY (col1) REFERENCES t2 (id),
  FOREIGN KEY (col2) REFERENCES t3 (id));
 
CREATE TABLE t5 (
  id INT,
  PRIMARY KEY (id));
  
CREATE TABLE t6 (
  id1 INT,
  id2 INT,
  PRIMARY KEY (id1, id2),
  FOREIGN KEY (id1) REFERENCES t5 (id));

CREATE TABLE t7 (
  id1 INT,
  id2 INT,
  id3 INT,
  PRIMARY KEY (id1, id2, id3),
  FOREIGN KEY (id1, id2) REFERENCES t6 (id1, id2));
`)
	if err != nil {
		tearDown()
		t.Fatal("fail to list tables: %w", err)
	}

	sut := schema.NewFetcher(db)

	got, err := sut.Fetch(context.Background())
	if err != nil {
		tearDown()
		t.Fatal("fail to list tables: %w", err)
	}
	wantTables := []schema2.Table{}
	wantReferences := [][]int{}
	if len(got.Tables()) != 7 {
		tearDown()
		t.Fatal(fmt.Sprintf(`too few tables: %d`, len(got.Tables())))
	}
	for _, want := range wantTables {
		if !containsTable(t, got.Tables(), want) {
			tearDown()
			t.Fatalf(`Table not found %s`, want.Name())
		}
	}

	if !equalsReferences(t, got.References(), wantReferences) {
		tearDown()
		t.Fatalf(`Referencces not match %#v`, wantReferences)
	}
}

func containsTable(t *testing.T, gotTables []schema2.Table, want schema2.Table) bool {
	t.Helper()
	return slices.ContainsFunc(gotTables, func(got schema2.Table) bool {
		return equalsTable(t, got, want)
	})
}

func equalsReferences(t *testing.T, got [][]int, want [][]int) bool {
	t.Helper()

	if len(got) != len(want) {
		return false
	}
	for i, got := range got {
		want := want[i]
		if len(got) != len(want) {
			return false
		}
		for j, got := range got {
			want := want[j]
			if got != want {
				return false
			}
		}
	}
	return true
}
func equalsTable(t *testing.T, got schema2.Table, want schema2.Table) bool {
	t.Helper()

	if got.Name() != want.Name() {
		return false
	}
	if !slices.EqualFunc(got.Columns(), want.Columns(), func(got, want schema2.Column) bool {
		return equalsColumn(t, got, want)
	}) {
		return false
	}
	if !slices.Equal(got.PrimaryKey(), want.PrimaryKey()) {
		return false
	}
	return true
}

func equalsColumn(t *testing.T, got schema2.Column, want schema2.Column) bool {
	t.Helper()
	if got.Name() != want.Name() {
		return false
	}
	if got.Type() != want.Type() {
		return false
	}
	if want.GoType().AssignableTo(got.GoType()) {
		return false
	}
	return true
}
