package dbsql_test

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/dbsql"
)

func newType[T any]() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}
func TestNewScanRowType(t *testing.T) {
	type S struct {
		ColString     string
		ColStringPtr  *string
		ColNullString sql.NullString

		ColBool     bool
		ColBoolPtr  *bool
		ColNullBool sql.NullBool

		ColFloat32    float32
		ColFloat32Ptr *float32
		ColFloat64    float64
		ColFloat64Ptr *float64
		ColNulFloat64 sql.NullFloat64

		ColInt       int
		ColIntPtr    *int
		ColUInt      uint
		ColUIntPtr   *uint
		ColInt8      int8
		ColInt8Ptr   *int8
		ColUInt8     uint8
		ColUInt8Ptr  *uint8
		ColInt16     int16
		ColInt16Ptr  *int16
		ColUInt16    uint16
		ColUInt16Ptr *uint16
		ColInt32     int32
		ColInt32Ptr  *int32
		ColUInt32    uint32
		ColUInt32Ptr *uint32
		ColInt64     int64
		ColInt64Ptr  *int64
		ColUInt64    uint64
		ColUInt64Ptr *uint64
		ColNullByte  sql.NullByte
		ColNullInt16 sql.NullInt16
		ColNullInt32 sql.NullInt32
		ColNullInt64 sql.NullInt64

		ColTime     time.Time
		ColTimePtr  *time.Time
		ColNullTime sql.NullTime

		ColBytes     dbsql.NullBytes
		ColBytesPtr  *dbsql.NullBytes
		ColNullBytes dbsql.NullBytes
	}

	want := dbsql.ScanRowTypes{
		"ColString":     newType[string](),
		"ColStringPtr":  newType[*string](),
		"ColNullString": newType[sql.NullString](),

		"ColBool":     newType[bool](),
		"ColBoolPtr":  newType[*bool](),
		"ColNullBool": newType[sql.NullBool](),

		"ColFloat32":    newType[float32](),
		"ColFloat32Ptr": newType[*float32](),
		"ColFloat64":    newType[float64](),
		"ColFloat64Ptr": newType[*float64](),
		"ColNulFloat64": newType[sql.NullFloat64](),

		"ColInt":       newType[int](),
		"ColIntPtr":    newType[*int](),
		"ColUInt":      newType[uint](),
		"ColUIntPtr":   newType[*uint](),
		"ColInt8":      newType[int8](),
		"ColInt8Ptr":   newType[*int8](),
		"ColUInt8":     newType[uint8](),
		"ColUInt8Ptr":  newType[*uint8](),
		"ColInt16":     newType[int16](),
		"ColInt16Ptr":  newType[*int16](),
		"ColUInt16":    newType[uint16](),
		"ColUInt16Ptr": newType[*uint16](),
		"ColInt32":     newType[int32](),
		"ColInt32Ptr":  newType[*int32](),
		"ColUInt32":    newType[uint32](),
		"ColUInt32Ptr": newType[*uint32](),
		"ColInt64":     newType[int64](),
		"ColInt64Ptr":  newType[*int64](),
		"ColUInt64":    newType[uint64](),
		"ColUInt64Ptr": newType[*uint64](),
		"ColNullByte":  newType[sql.NullByte](),
		"ColNullInt16": newType[sql.NullInt16](),
		"ColNullInt32": newType[sql.NullInt32](),
		"ColNullInt64": newType[sql.NullInt64](),

		"ColTime":     newType[time.Time](),
		"ColTimePtr":  newType[*time.Time](),
		"ColNullTime": newType[sql.NullTime](),

		"ColBytes":     newType[dbsql.NullBytes](),
		"ColBytesPtr":  newType[*dbsql.NullBytes](),
		"ColNullBytes": newType[dbsql.NullBytes](),
	}

	got := dbsql.NewScanRowTypes[S]()

	if len(got) != len(want) {
		t.Errorf(`len(got) = %d, len(want) = %d`, len(got), len(want))
	}
	for wantCol, wantTyp := range want {
		gotTyp, ok := got[wantCol]
		if !ok {
			t.Errorf(`got does not have key %v`, wantCol)
		}
		if gotTyp != wantTyp {
			t.Errorf(`got type for %v = %v, want type = %v`, wantCol, gotTyp, wantTyp)
		}
	}
}

func TestStructScanRowValue(t *testing.T) {
	type S struct {
		ColString     string
		ColStringPtr  *string
		ColNullString sql.NullString

		ColBool     bool
		ColBoolPtr  *bool
		ColNullBool sql.NullBool

		ColFloat32    float32
		ColFloat32Ptr *float32
		ColFloat64    float64
		ColFloat64Ptr *float64
		ColNulFloat64 sql.NullFloat64

		ColInt       int
		ColIntPtr    *int
		ColUInt      uint
		ColUIntPtr   *uint
		ColInt8      int8
		ColInt8Ptr   *int8
		ColUInt8     uint8
		ColUInt8Ptr  *uint8
		ColInt16     int16
		ColInt16Ptr  *int16
		ColUInt16    uint16
		ColUInt16Ptr *uint16
		ColInt32     int32
		ColInt32Ptr  *int32
		ColUInt32    uint32
		ColUInt32Ptr *uint32
		ColInt64     int64
		ColInt64Ptr  *int64
		ColUInt64    uint64
		ColUInt64Ptr *uint64
		ColNullByte  sql.NullByte
		ColNullInt16 sql.NullInt16
		ColNullInt32 sql.NullInt32
		ColNullInt64 sql.NullInt64

		ColTime     time.Time
		ColTimePtr  *time.Time
		ColNullTime sql.NullTime

		ColBytes     []byte
		ColBytesPtr  *[]byte
		ColNullBytes dbsql.NullBytes
	}

	type testCase struct {
		input map[string]any
		want  S
	}
	testCases := []testCase{
		{
			input: map[string]any{
				"ColString":     "",
				"ColStringPtr":  (*string)(nil),
				"ColNullString": sql.NullString{Valid: false},

				"ColBool":     false,
				"ColBoolPtr":  (*bool)(nil),
				"ColNullBool": sql.NullBool{Valid: false},

				"ColFloat32":    float32(0),
				"ColFloat32Ptr": (*float32)(nil),
				"ColFloat64":    float64(0),
				"ColFloat64Ptr": (*float64)(nil),
				"ColNulFloat64": sql.NullFloat64{Valid: false},

				"ColInt":       int(0),
				"ColIntPtr":    (*int)(nil),
				"ColUInt":      uint(0),
				"ColUIntPtr":   (*uint)(nil),
				"ColInt8":      int8(0),
				"ColInt8Ptr":   (*int8)(nil),
				"ColUInt8":     uint8(0),
				"ColUInt8Ptr":  (*uint8)(nil),
				"ColInt16":     int16(0),
				"ColInt16Ptr":  (*int16)(nil),
				"ColUInt16":    uint16(0),
				"ColUInt16Ptr": (*uint16)(nil),
				"ColInt32":     int32(0),
				"ColInt32Ptr":  (*int32)(nil),
				"ColUInt32":    uint32(0),
				"ColUInt32Ptr": (*uint32)(nil),
				"ColInt64":     int64(0),
				"ColInt64Ptr":  (*int64)(nil),
				"ColUInt64":    uint64(0),
				"ColUInt64Ptr": (*uint64)(nil),
				"ColNullByte":  sql.NullByte{Valid: false},
				"ColNullInt16": sql.NullInt16{Valid: false},
				"ColNullInt32": sql.NullInt32{Valid: false},
				"ColNullInt64": sql.NullInt64{Valid: false},

				"ColTime":     time.Time{},
				"ColTimePtr":  (*time.Time)(nil),
				"ColNullTime": sql.NullTime{Valid: false},

				"ColBytes":     []byte(nil),
				"ColBytesPtr":  (*[]byte)(nil),
				"ColNullBytes": dbsql.NullBytes{Valid: false},
			},
			want: S{
				ColString:     "",
				ColStringPtr:  (*string)(nil),
				ColNullString: sql.NullString{Valid: false},

				ColBool:     false,
				ColBoolPtr:  (*bool)(nil),
				ColNullBool: sql.NullBool{Valid: false},

				ColFloat32:    float32(0),
				ColFloat32Ptr: (*float32)(nil),
				ColFloat64:    float64(0),
				ColFloat64Ptr: (*float64)(nil),
				ColNulFloat64: sql.NullFloat64{Valid: false},

				ColInt:       int(0),
				ColIntPtr:    (*int)(nil),
				ColUInt:      uint(0),
				ColUIntPtr:   (*uint)(nil),
				ColInt8:      int8(0),
				ColInt8Ptr:   (*int8)(nil),
				ColUInt8:     uint8(0),
				ColUInt8Ptr:  (*uint8)(nil),
				ColInt16:     int16(0),
				ColInt16Ptr:  (*int16)(nil),
				ColUInt16:    uint16(0),
				ColUInt16Ptr: (*uint16)(nil),
				ColInt32:     int32(0),
				ColInt32Ptr:  (*int32)(nil),
				ColUInt32:    uint32(0),
				ColUInt32Ptr: (*uint32)(nil),
				ColInt64:     int64(0),
				ColInt64Ptr:  (*int64)(nil),
				ColUInt64:    uint64(0),
				ColUInt64Ptr: (*uint64)(nil),
				ColNullByte:  sql.NullByte{Valid: false},
				ColNullInt16: sql.NullInt16{Valid: false},
				ColNullInt32: sql.NullInt32{Valid: false},
				ColNullInt64: sql.NullInt64{Valid: false},

				ColTime:     time.Time{},
				ColTimePtr:  (*time.Time)(nil),
				ColNullTime: sql.NullTime{Valid: false},

				ColBytes:     []byte(nil),
				ColBytesPtr:  (*[]byte)(nil),
				ColNullBytes: dbsql.NullBytes{Valid: false},
			},
		},
	}

	for i, testCase := range testCases {
		got := dbsql.StructScanRowValue[S](testCase.input)
		want := testCase.want
		checkValue(t, i, got.ColString, want.ColString)
		checkValuePtr(t, i, got.ColStringPtr, want.ColStringPtr)
		checkValue(t, i, got.ColNullString, want.ColNullString)

		checkValue(t, i, got.ColBool, want.ColBool)
		checkValuePtr(t, i, got.ColBoolPtr, want.ColBoolPtr)
		checkValue(t, i, got.ColNullBool, want.ColNullBool)

		checkValue(t, i, got.ColFloat32, want.ColFloat32)
		checkValuePtr(t, i, got.ColFloat32Ptr, want.ColFloat32Ptr)
		checkValue(t, i, got.ColFloat64, want.ColFloat64)
		checkValuePtr(t, i, got.ColFloat64Ptr, want.ColFloat64Ptr)
		checkValue(t, i, got.ColNulFloat64, want.ColNulFloat64)

		checkValue(t, i, got.ColInt, want.ColInt)
		checkValuePtr(t, i, got.ColIntPtr, want.ColIntPtr)
		checkValue(t, i, got.ColUInt, want.ColUInt)
		checkValuePtr(t, i, got.ColUIntPtr, want.ColUIntPtr)
		checkValue(t, i, got.ColInt8, want.ColInt8)
		checkValuePtr(t, i, got.ColInt8Ptr, want.ColInt8Ptr)
		checkValue(t, i, got.ColUInt8, want.ColUInt8)
		checkValuePtr(t, i, got.ColInt8Ptr, want.ColInt8Ptr)
		checkValue(t, i, got.ColInt16, want.ColInt16)
		checkValuePtr(t, i, got.ColInt16Ptr, want.ColInt16Ptr)
		checkValue(t, i, got.ColUInt16, want.ColUInt16)
		checkValuePtr(t, i, got.ColInt16Ptr, want.ColInt16Ptr)
		checkValue(t, i, got.ColInt32, want.ColInt32)
		checkValuePtr(t, i, got.ColInt32Ptr, want.ColInt32Ptr)
		checkValue(t, i, got.ColUInt32, want.ColUInt32)
		checkValuePtr(t, i, got.ColInt32Ptr, want.ColInt32Ptr)
		checkValue(t, i, got.ColInt64, want.ColInt64)
		checkValuePtr(t, i, got.ColInt64Ptr, want.ColInt64Ptr)
		checkValue(t, i, got.ColUInt64, want.ColUInt64)
		checkValuePtr(t, i, got.ColInt64Ptr, want.ColInt64Ptr)
		checkValue(t, i, got.ColNullByte, want.ColNullByte)
		checkValue(t, i, got.ColNullInt16, want.ColNullInt16)
		checkValue(t, i, got.ColNullInt32, want.ColNullInt32)
		checkValue(t, i, got.ColNullInt64, want.ColNullInt64)

		checkValue(t, i, got.ColTime, want.ColTime)
		checkValuePtr(t, i, got.ColTimePtr, want.ColTimePtr)
		checkValue(t, i, got.ColNullTime, want.ColNullTime)

		checkBytes(t, i, got.ColBytes, want.ColBytes)
		checkBytesPtr(t, i, got.ColBytesPtr, want.ColBytesPtr)
		checkNullBytes(t, i, got.ColNullBytes, want.ColNullBytes)

	}
}

func checkValue[T comparable](t *testing.T, i int, got T, want T) {
	t.Helper()
	if got != want {
		t.Errorf("case %v:\n  got = %v\n  want = %v", i, got, want)
	}
}

func checkValuePtr[T comparable](t *testing.T, i int, got *T, want *T) {
	t.Helper()
	if (got == nil) != (want == nil) {
		t.Errorf("case %v:\n  got = %v\n  want = %v", i, got, want)
	}
	if got != nil {
		if *got != *want {
			t.Errorf("case %v:\n  *got = %v\n  *want = %v", i, *got, *want)
		}

	}
}

func checkBytes(t *testing.T, i int, got []byte, want []byte) {
	t.Helper()
	if string(got) != string(want) {
		t.Errorf("case %v:\n  got = %v\n  want = %v", i, got, want)
	}
}

func checkNullBytes(t *testing.T, i int, got dbsql.NullBytes, want dbsql.NullBytes) {
	t.Helper()
	if got.Valid != want.Valid {
		t.Errorf("case %v:\n  got = %v\n  want = %v", i, got, want)
	}
	if got.Valid {
		if string(got.Bytes) != string(want.Bytes) {
			t.Errorf("case %v:\n  *got = %v\n  *want = %v", i, got, want)
		}

	}
}

func checkBytesPtr(t *testing.T, i int, got *[]byte, want *[]byte) {
	t.Helper()
	if (got == nil) != (want == nil) {
		t.Errorf("case %v:\n  got = %v\n  want = %v", i, got, want)
	}
	if got != nil {
		if string(*got) != string(*want) {
			t.Errorf("case %v:\n  *got = %v\n  *want = %v", i, *got, *want)
		}
	}
}
