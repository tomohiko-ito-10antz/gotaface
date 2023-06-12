package dump_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/dml"
	schema_impl "github.com/Jumpaku/gotaface/spanner/ddl/schema"
	"github.com/Jumpaku/gotaface/spanner/dml/dump"
	"golang.org/x/exp/slices"

	spanner_test "github.com/Jumpaku/gotaface/spanner/test"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDumper_Dump_Order(t *testing.T) {
	adminClient, client, tearDown := spanner_test.Setup(t, fmt.Sprintf(`dml_dump_%d`, time.Now().UnixNano()))
	defer tearDown()

	spanner_test.InitDDL(t, adminClient, client.DatabaseName(), []string{`
CREATE TABLE t (
	id1 INT64,
	id2 INT64,
) PRIMARY KEY (id1, id2)
`})

	spanner_test.InitDML(t, client, []spanner.Statement{spanner.NewStatement(`
INSERT INTO t (id1, id2)
VALUES
	(1, 2),
	(2, 2),
	(1, 1),
	(2, 1)
`)})

	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	ctx := context.Background()
	schema, err := schema_impl.NewFetcher(tx).Fetch(ctx)
	if err != nil {
		t.Fatalf("fail to fetch schema: %v", err)
	}

	sut := dump.NewDumper(tx, schema)

	got, err := sut.Dump(context.Background(), `t`)
	if err != nil {
		t.Errorf("fail to dump table: %v", err)
	}

	want := dml.Rows{
		{
			`id1`: spanner.NullInt64{Valid: true, Int64: 1},
			`id2`: spanner.NullInt64{Valid: true, Int64: 1},
		}, {
			`id1`: spanner.NullInt64{Valid: true, Int64: 1},
			`id2`: spanner.NullInt64{Valid: true, Int64: 2},
		}, {
			`id1`: spanner.NullInt64{Valid: true, Int64: 2},
			`id2`: spanner.NullInt64{Valid: true, Int64: 1},
		}, {
			`id1`: spanner.NullInt64{Valid: true, Int64: 2},
			`id2`: spanner.NullInt64{Valid: true, Int64: 2},
		},
	}

	if len(got) != len(want) {
		t.Errorf("table count not match\n  got = %v\n  want = %v", got, want)
	}
	for i, want := range want {
		for key, want := range want {
			got, ok := got[i][key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s", i, key)
			}
			if !equals(got, want) {
				t.Errorf("i = %d, key = %s\n  gotVal  = %#v\n  wantVal = %#v", i, key, got, want)
			}
		}
	}
}

func TestDumper_Dump_Types(t *testing.T) {
	adminClient, client, tearDown := spanner_test.Setup(t, fmt.Sprintf(`dml_dump_%d`, time.Now().UnixNano()))
	defer tearDown()

	spanner_test.InitDDL(t, adminClient, client.DatabaseName(), []string{`
CREATE TABLE u (
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
`})

	spanner_test.InitDML(t, client, []spanner.Statement{spanner.NewStatement(`
INSERT INTO u (col_integer, col_string, col_float, col_bytes, col_bool, col_timestamp, col_date, col_json, col_array)
VALUES (1, 'abc', 1.25, b'1234abcd', TRUE,  '2023-06-11T01:23:45Z', '2023-06-11', JSON'{"a":1, "b":"x", "c":null, "d":[{}, []], "e":{"x":{}, "y":[]}}', [1, 2, 3])
`)})

	tx := client.ReadOnlyTransaction()
	defer tx.Close()

	ctx := context.Background()
	schema, err := schema_impl.NewFetcher(tx).Fetch(ctx)
	if err != nil {
		t.Fatal("fail to fetch schema: %w", err)
	}

	sut := dump.NewDumper(tx, schema)

	got, err := sut.Dump(context.Background(), `u`)
	if err != nil {
		t.Errorf("fail to dump table: %v", err)
	}
	wantTime := time.Date(2023, 6, 11, 1, 23, 45, 0, time.UTC)
	wantJSON := map[string]any{"a": 1, "b": "x", "c": nil, "d": []any{map[string]any{}, []any{}}, "e": map[string]any{"x": map[string]any{}, "y": []any{}}}
	want := dml.Rows{
		{
			`col_integer`:   spanner.NullInt64{Valid: true, Int64: 1},
			`col_string`:    spanner.NullString{Valid: true, StringVal: `abc`},
			`col_float`:     spanner.NullFloat64{Valid: true, Float64: 1.25},
			`col_bytes`:     []byte(`1234abcd`),
			`col_bool`:      spanner.NullBool{Valid: true, Bool: true},
			`col_timestamp`: spanner.NullTime{Valid: true, Time: wantTime},
			`col_date`:      spanner.NullDate{Valid: true, Date: civil.DateOf(wantTime)},
			`col_json`:      spanner.NullJSON{Valid: true, Value: wantJSON},
			`col_array`:     []spanner.NullInt64{{Valid: true, Int64: 1}, {Valid: true, Int64: 2}, {Valid: true, Int64: 3}},
		},
	}

	if len(got) != len(want) {
		t.Errorf("table count not match\n  len(got) = %v\n  len(want) = %v", len(got), len(want))
	}
	for i, want := range want {
		for key, want := range want {
			got, ok := got[i][key]
			if !ok {
				t.Errorf("i = %d: gotVal does not have key %s", i, key)
			}
			if !equals(got, want) {
				t.Errorf("i = %d, key = %s\n  gotVal  = %#v\n  wantVal = %#v", i, key, got, want)
			}
		}
	}
}

func equals(got any, want any) bool {
	switch want.(type) {
	default:
		return got == want
	case []byte:
		return slices.Equal(got.([]byte), want.([]byte))
	case spanner.NullJSON:
		got, _ := got.(spanner.NullJSON).MarshalJSON()
		want, _ := want.(spanner.NullJSON).MarshalJSON()
		return string(got) == string(want)
	case []spanner.NullInt64:
		return slices.Equal(got.([]spanner.NullInt64), want.([]spanner.NullInt64))
	}
}
