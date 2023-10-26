package insert_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml"
	"golang.org/x/exp/slices"

	"github.com/Jumpaku/gotaface/spanner/dml/insert"
	spanner_test "github.com/Jumpaku/gotaface/spanner/test"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
func TestInserter_Insert(t *testing.T) {
	adminClient, client, tearDown := spanner_test.Setup(t, fmt.Sprintf(`dml_insert_%d`, time.Now().UnixNano()))
	defer tearDown()

	spanner_test.InitDDL(t, adminClient, client.DatabaseName(), []string{`
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
`})

	timeValue := time.Date(2023, 6, 11, 1, 23, 45, 0, time.UTC)
	jsonValue := map[string]any{"a": 1, "b": "x", "c": nil, "d": []any{map[string]any{}, []any{}}, "e": map[string]any{"x": map[string]any{}, "y": []any{}}}
	rows := dml.Rows{
		{
			`col_integer`:   spanner.NullInt64{Valid: true, Int64: 1},
			`col_string`:    spanner.NullString{Valid: true, StringVal: `abc`},
			`col_float`:     spanner.NullFloat64{Valid: true, Float64: 1.25},
			`col_bytes`:     []byte(`1234abcd`),
			`col_bool`:      spanner.NullBool{Valid: true, Bool: true},
			`col_timestamp`: spanner.NullTime{Valid: true, Time: timeValue},
			`col_date`:      spanner.NullDate{Valid: true, Date: civil.DateOf(timeValue)},
			`col_json`:      spanner.NullJSON{Valid: true, Value: jsonValue},
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

	ctx := context.Background()
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		sut := insert.NewInserter(tx)

		err := sut.Insert(ctx, `t`, rows)
		if err != nil {
			return fmt.Errorf("fail to insert rows: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Errorf("fail to insert rows: %v", err)
	}

	for i, want := range rows {
		tx := client.ReadOnlyTransaction()
		defer tx.Close()

		type Row struct {
			Col_integer   spanner.NullInt64
			Col_string    spanner.NullString
			Col_float     spanner.NullFloat64
			Col_bytes     []byte
			Col_bool      spanner.NullBool
			Col_timestamp spanner.NullTime
			Col_date      spanner.NullDate
			Col_json      spanner.NullJSON
			Col_array     []spanner.NullInt64
		}

		want := Row{
			Col_integer:   want["col_integer"].(spanner.NullInt64),
			Col_string:    want["col_string"].(spanner.NullString),
			Col_float:     want["col_float"].(spanner.NullFloat64),
			Col_bytes:     want["col_bytes"].([]byte),
			Col_bool:      want["col_bool"].(spanner.NullBool),
			Col_timestamp: want["col_timestamp"].(spanner.NullTime),
			Col_date:      want["col_date"].(spanner.NullDate),
			Col_json:      want["col_json"].(spanner.NullJSON),
			Col_array:     want["col_array"].([]spanner.NullInt64),
		}
		found := spanner_test.FindRow[Row](t, tx, `t`, map[string]any{"col_integer": want.Col_integer.Int64})
		if found == nil {
			t.Errorf("not found: %v", want)
		}
		equals := true
		equals = equals && found.Col_integer == want.Col_integer
		equals = equals && found.Col_string == want.Col_string
		equals = equals && found.Col_float == want.Col_float
		equals = equals && slices.Equal(found.Col_bytes, want.Col_bytes)
		equals = equals && found.Col_bool == want.Col_bool
		equals = equals && found.Col_timestamp == want.Col_timestamp
		equals = equals && found.Col_date == want.Col_date
		gotJSON, _ := found.Col_json.MarshalJSON()
		wantJSON, _ := found.Col_json.MarshalJSON()
		equals = equals && string(gotJSON) == string(wantJSON)
		equals = equals && slices.Equal(found.Col_array, want.Col_array)
		if !equals {
			t.Errorf("i = %d, id1\n found = %#v\n   want = %#v", i, found, want)
		}
	}
}
