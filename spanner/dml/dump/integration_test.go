package dump_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/dml"
	schema_impl "github.com/Jumpaku/gotaface/spanner/ddl/schema"
	"github.com/Jumpaku/gotaface/spanner/dml/dump"

	spanner_test "github.com/Jumpaku/gotaface/spanner/test"

	spanner_adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	_ "github.com/mattn/go-sqlite3"
)

var (
	testSpannerProject  string
	testSpannerInstance string
)
var skipTest bool

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDumper_Dump(t *testing.T) {
	if skipTest {
		t.Skipf(`environment variables %s and %s are required`, spanner_test.EnvTestSpannerProject, spanner_test.EnvTestSpannerInstance)
	}

	spannerAdminClient, spannerClient, tearDown, err := spanner_test.Setup(testSpannerProject, testSpannerInstance, fmt.Sprintf(`dml_insert_%d`, time.Now().Unix()))
	if err != nil {
		t.Fatalf(`fail to set up spanner: %v`, err)
	}
	defer tearDown()

	ctx := context.Background()
	ddl := &spanner_adminpb.UpdateDatabaseDdlRequest{
		Database: spannerClient.DatabaseName(),
		Statements: []string{
			`
CREATE TABLE t (
	id1 INT64,
	id2 INT64,
	col_integer INT64,
	col_string STRING(MAX),
	col_float FLOAT64,
	col_bytes BYTES(16),
	col_bool BOOL,
	col_date DATE,
	col_timestamp TIMESTAMP
) PRIMARY KEY (id1, id2);
INSERT INTO t (id1, id2, col_integer, col_string, col_float, col_bytes, col_bool, col_date, col_timestamp)
VALUES
	(1, 2, 4, 'abc', 1.25, b'124abc', TRUE,  '2023-06-11T01:23:45Z', '2023-06-11T01:23:45Z'),
	(2, 2, 3, 'def', 1.50, b'223def', FALSE, '2023-06-11T01:23:45Z', '2023-06-11T01:23:45Z'),
	(1, 1, 2, 'ghi', 1.75, b'112ghi', TRUE,  '2023-06-11T01:23:45Z', '2023-06-11T01:23:45Z'),
	(2, 1, 1, 'jkl', 2.00, b'211jkl', FALSE, '2023-06-11T01:23:45Z', '2023-06-11T01:23:45Z');
`,
		},
	}
	op, err := spannerAdminClient.UpdateDatabaseDdl(ctx, ddl)
	if err != nil {
		tearDown()
		t.Fatalf(`fail to wait create tables: %v`, err)
	}
	if err := op.Wait(ctx); err != nil {
		tearDown()
		t.Fatalf(`fail to wait create tables: %v`, err)
	}

	tx := spannerClient.ReadOnlyTransaction()
	defer tx.Close()

	schema, err := schema_impl.NewFetcher(tx).Fetch(ctx)
	if err != nil {
		tearDown()
		t.Fatal("fail to fetch schema: %w", err)
	}

	sut := dump.NewDumper(tx, schema)

	got, err := sut.Dump(context.Background(), `t`)
	if err != nil {
		tx.Close()
		tearDown()
		t.Errorf("fail to dump table: %v", err)
	}
	wantTime := time.Date(2023, 6, 11, 1, 23, 45, 0, time.UTC)
	want := dml.Rows{
		{
			`id1`:           sql.NullInt64{Valid: true, Int64: 1},
			`id2`:           sql.NullInt64{Valid: true, Int64: 1},
			`col_integer`:   sql.NullInt64{Valid: true, Int64: 2},
			`col_string`:    sql.NullString{Valid: true, String: `ghi`},
			`col_float`:     sql.NullFloat64{Valid: true, Float64: 1.75},
			`col_bytes`:     []byte(`112ghi`),
			`col_bool`:      sql.NullBool{Valid: true, Bool: true},
			`col_date`:      sql.NullTime{Valid: true, Time: wantTime},
			`col_timestamp`: sql.NullTime{Valid: true, Time: wantTime},
		}, {
			`id1`:           sql.NullInt64{Valid: true, Int64: 1},
			`id2`:           sql.NullInt64{Valid: true, Int64: 2},
			`col_integer`:   sql.NullInt64{Valid: true, Int64: 4},
			`col_string`:    sql.NullString{Valid: true, String: `abc`},
			`col_float`:     sql.NullFloat64{Valid: true, Float64: 1.25},
			`col_bytes`:     []byte(`124abc`),
			`col_bool`:      sql.NullBool{Valid: true, Bool: true},
			`col_date`:      sql.NullTime{Valid: true, Time: wantTime},
			`col_timestamp`: sql.NullTime{Valid: true, Time: wantTime},
		}, {
			`id1`:           sql.NullInt64{Valid: true, Int64: 2},
			`id2`:           sql.NullInt64{Valid: true, Int64: 1},
			`col_integer`:   sql.NullInt64{Valid: true, Int64: 1},
			`col_string`:    sql.NullString{Valid: true, String: `jkl`},
			`col_float`:     sql.NullFloat64{Valid: true, Float64: 2.00},
			`col_bytes`:     []byte(`211jkl`),
			`col_bool`:      sql.NullBool{Valid: true, Bool: false},
			`col_date`:      sql.NullTime{Valid: true, Time: wantTime},
			`col_timestamp`: sql.NullTime{Valid: true, Time: wantTime},
		}, {
			`id1`:           sql.NullInt64{Valid: true, Int64: 2},
			`id2`:           sql.NullInt64{Valid: true, Int64: 2},
			`col_integer`:   sql.NullInt64{Valid: true, Int64: 3},
			`col_string`:    sql.NullString{Valid: true, String: `def`},
			`col_float`:     sql.NullFloat64{Valid: true, Float64: 1.50},
			`col_bytes`:     []byte(`223def`),
			`col_bool`:      sql.NullBool{Valid: true, Bool: false},
			`col_date`:      sql.NullTime{Valid: true, Time: wantTime},
			`col_timestamp`: sql.NullTime{Valid: true, Time: wantTime},
		},
	}

	if len(got) != len(want) {
		tx.Close()
		tearDown()
		t.Errorf("table count not match\n  len(got) = %v\n  len(want) = %v", len(got), len(want))
	}

	for i, want := range want {
		{
			key := "id1"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "id2"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_integer"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_string"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_float"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_bytes"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if string(gotVal.([]byte)) != string(want[key].([]byte)) {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_bool"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_date"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
		{
			key := "col_timestamp"
			got := got[i]
			gotVal, ok := got[key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s\n  gotVal  = %v\n  wantVal = %v", i, key, got, want)
			}
			if gotVal != want[key] {
				t.Errorf("i = %d: got[%s] != want[%s]\n  gotVal  = %#v\n  wantVal = %#v", i, key, key, got, want)
			}
		}
	}
}
