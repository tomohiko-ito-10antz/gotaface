package insert_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	spanner_adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/Jumpaku/gotaface/dml"
	"golang.org/x/exp/slices"

	"github.com/Jumpaku/gotaface/spanner/dml/insert"
	spanner_test "github.com/Jumpaku/gotaface/spanner/test"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
func TestInserter_Insert(t *testing.T) {
	adminClient, client, tearDown := spanner_test.Setup(t, fmt.Sprintf(`dml_dump_%d`, time.Now().Unix()))
	defer tearDown()

	ctx := context.Background()
	op, err := adminClient.UpdateDatabaseDdl(ctx, &spanner_adminpb.UpdateDatabaseDdlRequest{
		Database: client.DatabaseName(),
		Statements: []string{`
CREATE TABLE t (
	col_integer   INT64,
	col_string    STRING(MAX),
	col_float     FLOAT64,
	col_bytes     BYTES(16),
	col_bool      BOOL,
	col_timestamp TIMESTAMP,
	col_date      DATE,
	col_json      JSON,
	col_array    ARRAY<INT64>,
) PRIMARY KEY (col_integer)
`},
	})
	if err != nil {
		tearDown()
		t.Fatalf(`fail to wait create tables: %v`, err)
	}
	if err := op.Wait(ctx); err != nil {
		tearDown()
		t.Fatalf(`fail to wait create tables: %v`, err)
	}

	wantTime := time.Date(2023, 6, 11, 1, 23, 45, 0, time.UTC)
	rows := dml.Rows{
		{
			`col_integer`:   spanner.NullInt64{Valid: true, Int64: 1},
			`col_string`:    spanner.NullString{Valid: true, StringVal: `abc`},
			`col_float`:     spanner.NullFloat64{Valid: true, Float64: 1.25},
			`col_bytes`:     []byte(`1234abcd`),
			`col_bool`:      spanner.NullBool{Valid: true, Bool: true},
			`col_timestamp`: spanner.NullTime{Valid: true, Time: wantTime},
			`col_date`:      spanner.NullDate{Valid: true, Date: civil.DateOf(wantTime)},
			`col_json`:      spanner.NullJSON{Valid: true, Value: map[string]any{"a": 1, "b": "x", "c": nil, "d": []any{map[string]any{}, []any{}}, "e": map[string]any{"x": map[string]any{}, "y": []any{}}}},
			`col_array`:     []spanner.NullInt64{{Valid: true, Int64: 1}, {Valid: true, Int64: 2}, {Valid: true, Int64: 3}},
		},
		{
			`col_integer`:   spanner.NullInt64{Valid: true, Int64: 2},
			`col_string`:    spanner.NullString{Valid: true, StringVal: `def`},
			`col_float`:     spanner.NullFloat64{Valid: true, Float64: 1.50},
			`col_bytes`:     []byte(`1234abcd`),
			`col_bool`:      spanner.NullBool{Valid: true, Bool: false},
			`col_timestamp`: spanner.NullTime{Valid: false},
			`col_date`:      spanner.NullDate{Valid: false},
			`col_json`:      spanner.NullJSON{Valid: false},
			`col_array`:     []spanner.NullInt64{{Valid: false}, {Valid: false}, {Valid: false}},
		},
	}

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		sut := insert.NewInserter(tx)
		err := sut.Insert(ctx, `t`, rows)
		if err != nil {
			return fmt.Errorf("fail to insert rows: %w", err)
		}
		return nil
	})
	if err != nil {
		tearDown()
		t.Errorf("fail to insert rows: %v", err)
	}

	for i, want := range rows {
		tx := client.ReadOnlyTransaction()
		defer tx.Close()
		got, err := tx.Query(ctx, spanner.Statement{
			SQL:    `SELECT col_integer, col_string, col_float, col_bytes, col_bool, col_timestamp, col_date, col_json, col_array FROM t WHERE col_integer = @Key`,
			Params: map[string]any{"Key": want["col_integer"].(spanner.NullInt64).Int64},
		}).Next()
		if err != nil {
			tx.Close()
			tearDown()
			t.Fatalf("fail to select row: %v", err)
		}
		var (
			col_integer   spanner.NullInt64
			col_string    spanner.NullString
			col_float     spanner.NullFloat64
			col_bytes     []byte
			col_bool      spanner.NullBool
			col_timestamp spanner.NullTime
			col_date      spanner.NullDate
			col_json      spanner.NullJSON
			col_array     []spanner.NullInt64
		)

		err = got.Columns(&col_integer, &col_string, &col_float, &col_bytes, &col_bool, &col_timestamp, &col_date, &col_json, &col_array)
		if err != nil {
			tearDown()
			t.Fatalf("fail to scan row: %v", err)
		}
		{
			if key := "col_integer"; col_integer != want[key] {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_integer, want[key])
			}
		}
		{
			if key := "col_string"; col_string != want[key] {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_string, want[key])
			}
		}
		{
			if key := "col_float"; col_float != want[key] {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_float, want[key])
			}
		}
		{
			if key := "col_bytes"; string(col_bytes) != string(want[key].([]byte)) {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_bytes, want[key])
			}
		}
		{
			if key := "col_bool"; col_bool != want[key] {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_bool, want[key])
			}
		}
		{
			if key := "col_timestamp"; col_timestamp != want[key] {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_timestamp, want[key])
			}
		}
		{
			if key := "col_date"; col_date != want[key] {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_date, want[key])
			}
		}
		{
			if key := "col_json"; col_json.String() != (want[key].(spanner.NullJSON).String()) {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_json, want[key])
			}
		}
		{
			if key := "col_array"; !slices.Equal(col_array, want[key].([]spanner.NullInt64)) {
				t.Errorf("i = %d, key = %s\n  column = %#v\n  want   = %#v", i, key, col_array, want[key])
			}
		}
	}
}
