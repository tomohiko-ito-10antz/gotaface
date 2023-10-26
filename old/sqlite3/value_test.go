package sqlite3_test

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Jumpaku/gotaface/old/sqlite3"
	"golang.org/x/exp/slices"
)

func TestGoType(t *testing.T) {
	type testCase struct {
		colType string
		want    reflect.Type
	}
	testCases := []testCase{
		{
			colType: "INT",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "INTEGER",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "TINYINT",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "SMALLINT",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "MEDIUMINT",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "BIGINT",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "UNSIGNED BIG INT",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "INT2",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "INT8",
			want:    sqlite3.RefType[sql.NullInt64](),
		}, {
			colType: "CHARACTER(20)",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "VARCHAR(255)",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "VARYING CHARACTER(255)",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "NCHAR(55)",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "NATIVE CHARACTER(70)",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "NVARCHAR(100)",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "TEXT",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "CLOB",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "BLOB",
			want:    sqlite3.RefType[[]byte](),
		}, {
			colType: "",
			want:    sqlite3.RefType[[]byte](),
		}, {
			colType: "REAL",
			want:    sqlite3.RefType[sql.NullFloat64](),
		}, {
			colType: "DOUBLE",
			want:    sqlite3.RefType[sql.NullFloat64](),
		}, {
			colType: "DOUBLE PRECISION",
			want:    sqlite3.RefType[sql.NullFloat64](),
		}, {
			colType: "FLOAT",
			want:    sqlite3.RefType[sql.NullFloat64](),
		}, {
			colType: "NUMERIC",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "DECIMAL(10,5)",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "BOOLEAN",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "DATE",
			want:    sqlite3.RefType[sql.NullString](),
		}, {
			colType: "DATETIME",
			want:    sqlite3.RefType[sql.NullString](),
		},
	}

	for _, testCase := range testCases {
		got := sqlite3.GoType(testCase.colType)
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
	case []byte:
		got, ok := got.([]byte)
		return ok && (got == nil) == (want == nil) && slices.Equal(got, want)
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

func TestToDBValue_INT(t *testing.T) {
	testCases := []testCaseToDBValue{
		{
			typ:  "INT",
			src:  nil,
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  json.Number("123"),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(json.Number("123")),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  (*json.Number)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  int(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  int8(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  int16(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  int32(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  int64(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  uint(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  uint8(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  uint16(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  uint32(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  uint64(123),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(int(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(int8(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(int16(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(int32(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(int64(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(uint(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(uint8(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(uint16(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(uint32(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  ptr(uint64(123)),
			want: sql.NullInt64{Valid: true, Int64: 123},
		}, {
			typ:  "INT",
			src:  (*int)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*int8)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*int16)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*int32)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*int64)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*uint)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*uint8)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*uint16)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*uint32)(nil),
			want: sql.NullInt64{},
		}, {
			typ:  "INT",
			src:  (*uint64)(nil),
			want: sql.NullInt64{},
		},
	}

	for i, testCase := range testCases {
		got, err := sqlite3.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_FLOAT64(t *testing.T) {
	testCases := []testCaseToDBValue{
		// FLOAT64
		{
			typ:  "FLOAT",
			src:  nil,
			want: sql.NullFloat64{},
		}, {
			typ:  "FLOAT",
			src:  json.Number("-0.25"),
			want: sql.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT",
			src:  ptr(json.Number("-0.25")),
			want: sql.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT",
			src:  (*json.Number)(nil),
			want: sql.NullFloat64{},
		}, {
			typ:  "FLOAT",
			src:  float32(-0.25),
			want: sql.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT",
			src:  float64(-0.25),
			want: sql.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT",
			src:  ptr(float32(-0.25)),
			want: sql.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT",
			src:  ptr(float64(-0.25)),
			want: sql.NullFloat64{Valid: true, Float64: -0.25},
		}, {
			typ:  "FLOAT",
			src:  (*float32)(nil),
			want: sql.NullFloat64{},
		}, {
			typ:  "FLOAT",
			src:  (*float64)(nil),
			want: sql.NullFloat64{},
		},
	}

	for i, testCase := range testCases {
		got, err := sqlite3.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_BYTES(t *testing.T) {
	testCases := []testCaseToDBValue{
		// BYTES
		{
			typ:  "BLOB",
			src:  nil,
			want: []byte(nil),
		}, {
			typ:  "BLOB",
			src:  []byte{},
			want: []byte{},
		}, {
			typ:  "BLOB",
			src:  []byte(nil),
			want: []byte(nil),
		}, {
			typ:  "BLOB",
			src:  `YWJj`,
			want: []byte(`abc`),
		}, {
			typ:  "BLOB",
			src:  ptr(`YWJj`),
			want: []byte(`abc`),
		}, {
			typ:  "BLOB",
			src:  (*string)(nil),
			want: []byte(nil),
		},
	}

	for i, testCase := range testCases {
		got, err := sqlite3.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}

func TestToDBValue_STRING(t *testing.T) {
	testCases := []testCaseToDBValue{
		// FLOAT64
		{
			typ:  "STRING(MAX)",
			src:  nil,
			want: sql.NullString{},
		}, {
			typ:  "STRING(MAX)",
			src:  "abc",
			want: sql.NullString{Valid: true, String: "abc"},
		}, {
			typ:  "STRING(MAX)",
			src:  ptr("abc"),
			want: sql.NullString{Valid: true, String: "abc"},
		}, {
			typ:  "STRING(MAX)",
			src:  (*string)(nil),
			want: sql.NullString{},
		},
	}

	for i, testCase := range testCases {
		got, err := sqlite3.ToDBValue(testCase.typ, testCase.src)
		checkTestCase(t, i, got, err, testCase)
	}
}
