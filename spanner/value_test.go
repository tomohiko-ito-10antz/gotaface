package spanner_test

import (
	"encoding/json"
	"math/big"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	spanner_impl "github.com/Jumpaku/gotaface/spanner"
	"golang.org/x/exp/slices"
)

func TestGoType(t *testing.T) {
	type testCase struct {
		input string
		want  reflect.Type
	}
	testCases := []testCase{
		{
			input: "INT64",
			want:  spanner_impl.RefType[spanner.NullInt64](),
		}, {
			input: "STRING(MAX)",
			want:  spanner_impl.RefType[spanner.NullString](),
		}, {
			input: "BOOL",
			want:  spanner_impl.RefType[spanner.NullBool](),
		}, {
			input: "FLOAT64",
			want:  spanner_impl.RefType[spanner.NullFloat64](),
		}, {
			input: "TIMESTAMP",
			want:  spanner_impl.RefType[spanner.NullTime](),
		}, {
			input: "DATE",
			want:  spanner_impl.RefType[spanner.NullDate](),
		}, {
			input: "NUMERIC",
			want:  spanner_impl.RefType[spanner.NullNumeric](),
		}, {
			input: "BYTES(255)",
			want:  spanner_impl.RefType[[]byte](),
		}, {
			input: "JSON",
			want:  spanner_impl.RefType[spanner.NullJSON](),
		}, {
			input: "ARRAY<INT64>",
			want:  spanner_impl.RefType[[]spanner.NullInt64](),
		}, {
			input: "ARRAY<STRUCT<INT64>>",
			want:  spanner_impl.RefType[[]spanner.NullRow](),
		}, {
			input: "STRUCT<x INT64, y STRING(12)>",
			want:  spanner_impl.RefType[spanner.NullRow](),
		}, {
			input: "STRUCT<ARRAY<INT64>>",
			want:  spanner_impl.RefType[spanner.NullRow](),
		},
	}

	for _, testCase := range testCases {
		got := spanner_impl.GoType(testCase.input)
		if got.Name() != testCase.want.Name() {
			t.Errorf("GoType() mismatch\n  got  = %v\n  want = %v", got, testCase.want)
		}
	}
}

func ptr[T any](v T) *T {
	return &v
}

type testCaseToDBValue struct {
	typ   string
	src   any
	want  any
	isErr bool
}

func equals(got any, want any) bool {
	// Slices
	if wantRV := reflect.ValueOf(want); wantRV.IsValid() && wantRV.Kind() == reflect.Slice {
		gotRV := reflect.ValueOf(got)
		if !gotRV.IsValid() || gotRV.Kind() != reflect.Slice {
			return false
		}
		if wantRV.Len() != gotRV.Len() {
			return false
		}
		for i := 0; i < wantRV.Len(); i++ {
			if !equals(gotRV.Index(i).Interface(), wantRV.Index(i).Interface()) {
				return false
			}
		}
		return true
	}

	switch want := want.(type) {
	default:
		return got == want
	case spanner.NullNumeric:
		got, ok := got.(spanner.NullNumeric)
		return ok && got.Valid == want.Valid && got.Numeric.Cmp(&want.Numeric) == 0
	case []byte:
		got, ok := got.([]byte)
		return ok && (got == nil) == (want == nil) && slices.Equal(got, want)
	case spanner.NullJSON:
		got, ok := got.(spanner.NullJSON)
		gotJSON, _ := got.MarshalJSON()
		wantJSON, _ := want.MarshalJSON()
		return ok && got.Valid == want.Valid && string(gotJSON) == string(wantJSON)
	case spanner.NullRow:
		got, ok := got.(spanner.NullRow)
		if !(ok && got.Valid == want.Valid && got.Row.Size() == want.Row.Size() && slices.Equal(got.Row.ColumnNames(), want.Row.ColumnNames())) {
			return false
		}
		for _, columnName := range want.Row.ColumnNames() {
			var gotVal any
			var wantVal any
			_ = want.Row.ColumnByName(columnName, &wantVal)
			_ = got.Row.ColumnByName(columnName, &gotVal)
			if !equals(gotVal, wantVal) {
				return false
			}
		}
		return true
	}
}
func checkTestCase(t *testing.T, i int, got any, err error, testCase testCaseToDBValue) {
	t.Helper()

	if testCase.isErr != (err != nil) {
		t.Errorf("i = %d: fail to convert to DB value\n  err = %v", i, err)
	}
	if !equals(got, testCase.want) {
		if got != testCase.want {
			t.Errorf("i = %d: mismatch\n  got  = %#v\n  want = %#v", i, got, testCase.want)
		}
	}
}

func TestToDBValue_INT64(t *testing.T) {
	testCases := []testCaseToDBValue{
		{
			typ:  "INT64",
			src:  nil,
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  json.Number("123"),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(json.Number("123")),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  (*json.Number)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  int(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  int8(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  int16(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  int32(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  int64(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  uint(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  uint8(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  uint16(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  uint32(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  uint64(123),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(int(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(int8(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(int16(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(int32(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(int64(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(uint(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(uint8(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(uint16(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(uint32(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  ptr(uint64(123)),
			want: spanner.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT64",
			src:  (*int)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*int8)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*int16)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*int32)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*int64)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*uint)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*uint8)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*uint16)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*uint32)(nil),
			want: spanner.NullInt64{},
		}, {
			typ:  "INT64",
			src:  (*uint64)(nil),
			want: spanner.NullInt64{},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_BOOL(t *testing.T) {
	testCases := []testCaseToDBValue{
		// BOOL
		{
			typ:  "BOOL",
			src:  nil,
			want: spanner.NullBool{},
		}, {
			typ:  "BOOL",
			src:  true,
			want: spanner.NullBool{Valid: true, Bool: true},
		}, {
			typ:  "BOOL",
			src:  ptr(true),
			want: spanner.NullBool{Valid: true, Bool: true},
		}, {
			typ:  "BOOL",
			src:  (*bool)(nil),
			want: spanner.NullBool{},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_STRING(t *testing.T) {
	testCases := []testCaseToDBValue{
		// FLOAT64
		{
			typ:  "STRING(MAX)",
			src:  nil,
			want: spanner.NullString{},
		}, {
			typ:  "STRING(MAX)",
			src:  "abc",
			want: spanner.NullString{Valid: true, StringVal: "abc"},
		}, {
			typ:  "STRING(MAX)",
			src:  ptr("abc"),
			want: spanner.NullString{Valid: true, StringVal: "abc"},
		}, {
			typ:  "STRING(MAX)",
			src:  (*string)(nil),
			want: spanner.NullString{},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_FLOAT64(t *testing.T) {
	testCases := []testCaseToDBValue{
		// FLOAT64
		{
			typ:  "FLOAT64",
			src:  nil,
			want: spanner.NullFloat64{},
		}, {
			typ:  "FLOAT64",
			src:  json.Number("-0.25"),
			want: spanner.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT64",
			src:  ptr(json.Number("-0.25")),
			want: spanner.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT64",
			src:  (*json.Number)(nil),
			want: spanner.NullFloat64{},
		}, {
			typ:  "FLOAT64",
			src:  float32(-0.25),
			want: spanner.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT64",
			src:  float64(-0.25),
			want: spanner.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT64",
			src:  ptr(float32(-0.25)),
			want: spanner.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT64",
			src:  ptr(float64(-0.25)),
			want: spanner.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT64",
			src:  (*float32)(nil),
			want: spanner.NullFloat64{},
		}, {
			typ:  "FLOAT64",
			src:  (*float64)(nil),
			want: spanner.NullFloat64{},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_TIMESTAMP(t *testing.T) {
	testTime, _ := time.Parse(time.RFC3339, "2023-07-02T13:20:15Z")
	testCases := []testCaseToDBValue{
		// TIMESTAMP
		{
			typ:  "TIMESTAMP",
			src:  nil,
			want: spanner.NullTime{},
		}, {
			typ:  "TIMESTAMP",
			src:  testTime,
			want: spanner.NullTime{Valid: true, Time: testTime},
		}, {
			typ:  "TIMESTAMP",
			src:  ptr(testTime),
			want: spanner.NullTime{Valid: true, Time: testTime},
		}, {
			typ:  "TIMESTAMP",
			src:  (*time.Time)(nil),
			want: spanner.NullTime{},
		}, {
			typ:  "TIMESTAMP",
			src:  "2023-07-02T13:20:15Z",
			want: spanner.NullTime{Valid: true, Time: testTime},
		}, {
			typ:  "TIMESTAMP",
			src:  ptr("2023-07-02T13:20:15Z"),
			want: spanner.NullTime{Valid: true, Time: testTime},
		}, {
			typ:  "TIMESTAMP",
			src:  (*string)(nil),
			want: spanner.NullTime{},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_DATE(t *testing.T) {
	testTime, _ := time.Parse(time.RFC3339, "2023-07-02T13:20:15Z")
	testCases := []testCaseToDBValue{
		// DATE
		{
			typ:  "DATE",
			src:  nil,
			want: spanner.NullDate{},
		}, {
			typ:  "DATE",
			src:  civil.DateOf(testTime),
			want: spanner.NullDate{Valid: true, Date: civil.DateOf(testTime)},
		}, {
			typ:  "DATE",
			src:  ptr(civil.DateOf(testTime)),
			want: spanner.NullDate{Valid: true, Date: civil.DateOf(testTime)},
		}, {
			typ:  "DATE",
			src:  (*civil.Date)(nil),
			want: spanner.NullDate{},
		}, {
			typ:  "DATE",
			src:  "2023-07-02",
			want: spanner.NullDate{Valid: true, Date: civil.DateOf(testTime)},
		}, {
			typ:  "DATE",
			src:  ptr("2023-07-02"),
			want: spanner.NullDate{Valid: true, Date: civil.DateOf(testTime)},
		},
		{
			typ:  "DATE",
			src:  (*string)(nil),
			want: spanner.NullDate{},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_NUMERIC(t *testing.T) {
	testCases := []testCaseToDBValue{
		// NUMERIC
		{
			typ:  "NUMERIC",
			src:  nil,
			want: spanner.NullNumeric{},
		},
		// int
		{
			typ:  "NUMERIC",
			src:  json.Number("123"),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(json.Number("123")),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  (*json.Number)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  int(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  int8(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  int16(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  int32(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  int64(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  uint(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  uint8(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  uint16(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  uint32(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  uint64(123),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(int(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(int8(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(int16(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(int32(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(int64(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(uint(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(uint8(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(uint16(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(uint32(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(uint64(123)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 1)},
		}, {
			typ:  "NUMERIC",
			src:  (*int)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*int8)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*int16)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*int32)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*int64)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*uint)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*uint8)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*uint16)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*uint32)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*uint64)(nil),
			want: spanner.NullNumeric{},
		},
		// float
		{
			typ:  "NUMERIC",
			src:  json.Number("-0.25"),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(-1, 4)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(json.Number("-0.25")),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(-1, 4)},
		}, {
			typ:  "NUMERIC",
			src:  (*json.Number)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  float32(-0.25),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(-1, 4)},
		}, {
			typ:  "NUMERIC",
			src:  float64(-0.25),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(-1, 4)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(float32(-0.25)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(-1, 4)},
		}, {
			typ:  "NUMERIC",
			src:  ptr(float64(-0.25)),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(-1, 4)},
		}, {
			typ:  "NUMERIC",
			src:  (*float32)(nil),
			want: spanner.NullNumeric{},
		}, {
			typ:  "NUMERIC",
			src:  (*float64)(nil),
			want: spanner.NullNumeric{},
		},
		// big.Rat
		{
			typ:  "NUMERIC",
			src:  *big.NewRat(123, 4),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 4)},
		}, {
			typ:  "NUMERIC",
			src:  big.NewRat(123, 4),
			want: spanner.NullNumeric{Valid: true, Numeric: *big.NewRat(123, 4)},
		}, {
			typ:  "NUMERIC",
			src:  (*big.Rat)(nil),
			want: spanner.NullNumeric{},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_BYTES(t *testing.T) {
	testCases := []testCaseToDBValue{
		// BYTES
		{
			typ:  "BYTES(255)",
			src:  nil,
			want: []byte(nil),
		}, {
			typ:  "BYTES(255)",
			src:  []byte{},
			want: []byte{},
		}, {
			typ:  "BYTES(255)",
			src:  []byte(nil),
			want: []byte(nil),
		}, {
			typ:  "BYTES(255)",
			src:  `YWJj`,
			want: []byte(`abc`),
		}, {
			typ:  "BYTES(255)",
			src:  ptr(`YWJj`),
			want: []byte(`abc`),
		}, {
			typ:  "BYTES(255)",
			src:  (*string)(nil),
			want: []byte(nil),
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}
func TestToDBValue_JSON(t *testing.T) {
	testJSON := map[string]any{"a": 1, "b": "x", "c": nil, "d": []any{map[string]any{}, []any{}}, "e": map[string]any{"x": map[string]any{}, "y": []any{}}}
	testCases := []testCaseToDBValue{
		// JSON
		{
			typ:  "JSON",
			src:  nil,
			want: spanner.NullJSON{},
		}, {
			typ:  "JSON",
			src:  testJSON,
			want: spanner.NullJSON{Valid: true, Value: testJSON},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_ARRAY(t *testing.T) {
	testCases := []testCaseToDBValue{
		// ARRAY
		{
			typ:  "ARRAY<INT64>",
			src:  nil,
			want: []spanner.NullInt64(nil),
		}, {
			typ:  "ARRAY<INT64>",
			src:  []int(nil),
			want: []spanner.NullInt64(nil),
		}, {
			typ:  "ARRAY<INT64>",
			src:  []any{},
			want: []spanner.NullInt64{},
		}, {
			typ:  "ARRAY<INT64>",
			src:  []any{nil, (*int)(nil), ptr(1), 2, json.Number("3")},
			want: []spanner.NullInt64{{}, {}, {Valid: true, Int64: 1}, {Valid: true, Int64: 2}, {Valid: true, Int64: 3}},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_STRUCT(t *testing.T) {
	rowPtr, _ := spanner.NewRow([]string{"x", "y"}, []any{int64(1), `abc`})
	testRow := *rowPtr
	testCases := []testCaseToDBValue{
		// STRUCT
		{
			typ:  "STRUCT<x INT64, y STRING(12)>",
			src:  nil,
			want: spanner.NullRow{},
		}, {
			typ:  "STRUCT<x INT64, y STRING(12)>",
			src:  testRow,
			want: spanner.NullRow{Valid: true, Row: testRow},
		}, {
			typ:  "STRUCT<x INT64, y STRING(12)>",
			src:  ptr(testRow),
			want: spanner.NullRow{Valid: true, Row: testRow},
		}, {
			typ:  "STRUCT<x INT64, y STRING(12)>",
			src:  (*spanner.Row)(nil),
			want: spanner.NullRow{},
		}, {
			typ:  "STRUCT<x INT64, y STRING(12)>",
			src:  spanner.NullRow{Valid: true, Row: testRow},
			want: spanner.NullRow{Valid: true, Row: testRow},
		}, {
			typ:  "STRUCT<x INT64, y STRING(12)>",
			src:  spanner.NullRow{},
			want: spanner.NullRow{},
		},
	}

	for i, testCase := range testCases {
		got, err := spanner_impl.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}
