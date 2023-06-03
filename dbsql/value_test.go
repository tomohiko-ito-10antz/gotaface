package dbsql_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Jumpaku/gotaface/dbsql"
)

func ptr[T any](s T) *T { return &s }

func TestValue_ToNullString(t *testing.T) {
	type Want struct {
		value sql.NullString
		ok    bool
	}
	type TestCase struct {
		sut  *dbsql.Value
		want Want
	}
	testCases := []TestCase{
		{
			sut:  &dbsql.Value{Any: nil},
			want: Want{value: sql.NullString{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: "abc"},
			want: Want{value: sql.NullString{Valid: true, String: "abc"}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*string)(nil)},
			want: Want{value: sql.NullString{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr("abc")},
			want: Want{value: sql.NullString{Valid: true, String: "abc"}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullString{Valid: false}},
			want: Want{value: sql.NullString{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullString{Valid: true, String: "abc"}},
			want: Want{value: sql.NullString{Valid: true, String: "abc"}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: struct{}{}},
			want: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		got, ok := testCase.sut.ToNullString()

		if ok != testCase.want.ok {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, ok, testCase.want.ok)
		}
		if ok && got != testCase.want.value {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, got, testCase.want.value)
		}
	}
}

func TestValue_ToNullBool(t *testing.T) {
	type Want struct {
		value sql.NullBool
		ok    bool
	}
	type TestCase struct {
		sut  *dbsql.Value
		want Want
	}

	testCases := []TestCase{
		{
			sut:  &dbsql.Value{Any: nil},
			want: Want{value: sql.NullBool{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: true},
			want: Want{value: sql.NullBool{Valid: true, Bool: true}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: false},
			want: Want{value: sql.NullBool{Valid: true, Bool: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*bool)(nil)},
			want: Want{value: sql.NullBool{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(true)},
			want: Want{value: sql.NullBool{Valid: true, Bool: true}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(false)},
			want: Want{value: sql.NullBool{Valid: true, Bool: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullBool{Valid: false}},
			want: Want{value: sql.NullBool{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullBool{Valid: true, Bool: true}},
			want: Want{value: sql.NullBool{Valid: true, Bool: true}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullBool{Valid: true, Bool: false}},
			want: Want{value: sql.NullBool{Valid: true, Bool: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: struct{}{}},
			want: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		got, ok := testCase.sut.ToNullBool()

		if ok != testCase.want.ok {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, ok, testCase.want.ok)
		}
		if ok && got != testCase.want.value {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, got, testCase.want.value)
		}
	}
}

func TestValue_ToNullFloat64(t *testing.T) {
	type Want struct {
		value sql.NullFloat64
		ok    bool
	}
	type TestCase struct {
		sut  *dbsql.Value
		want Want
	}

	testCases := []TestCase{
		{
			sut:  &dbsql.Value{Any: nil},
			want: Want{value: sql.NullFloat64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: float32(-0.5)},
			want: Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: float64(-0.5)},
			want: Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*float32)(nil)},
			want: Want{value: sql.NullFloat64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*float64)(nil)},
			want: Want{value: sql.NullFloat64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(float32(-0.5))},
			want: Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(float64(-0.5))},
			want: Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullFloat64{Valid: false}},
			want: Want{value: sql.NullFloat64{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullFloat64{Valid: true, Float64: -0.5}},
			want: Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: struct{}{}},
			want: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		got, ok := testCase.sut.ToNullFloat64()

		if ok != testCase.want.ok {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, ok, testCase.want.ok)
		}
		if ok && got != testCase.want.value {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, got, testCase.want.value)
		}
	}
}

func TestValue_ToNullTime(t *testing.T) {
	type Want struct {
		value sql.NullTime
		ok    bool
	}
	type TestCase struct {
		sut  *dbsql.Value
		want Want
	}

	date := time.Date(2023, 5, 31, 1, 2, 3, 4, time.Local)

	testCases := []TestCase{
		{
			sut:  &dbsql.Value{Any: nil},
			want: Want{value: sql.NullTime{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: date},
			want: Want{value: sql.NullTime{Valid: true, Time: date}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*time.Time)(nil)},
			want: Want{value: sql.NullTime{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(date)},
			want: Want{value: sql.NullTime{Valid: true, Time: date}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullTime{Valid: false}},
			want: Want{value: sql.NullTime{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullTime{Valid: true, Time: date}},
			want: Want{value: sql.NullTime{Valid: true, Time: date}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: struct{}{}},
			want: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		got, ok := testCase.sut.ToNullTime()

		if ok != testCase.want.ok {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, ok, testCase.want.ok)
		}
		if ok && got != testCase.want.value {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, got, testCase.want.value)
		}
	}
}

func TestValue_ToNullInt64(t *testing.T) {
	type Want struct {
		value sql.NullInt64
		ok    bool
	}
	type TestCase struct {
		sut  *dbsql.Value
		want Want
	}

	testCases := []TestCase{
		{
			sut:  &dbsql.Value{Any: nil},
			want: Want{value: sql.NullInt64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: int(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: int8(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: int16(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: int32(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: int64(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: uint(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: uint8(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: uint16(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: uint32(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: uint64(123)},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*int)(nil)},
			want: Want{value: sql.NullInt64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*int8)(nil)},
			want: Want{value: sql.NullInt64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*int16)(nil)},
			want: Want{value: sql.NullInt64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*int32)(nil)},
			want: Want{value: sql.NullInt64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*int64)(nil)},
			want: Want{value: sql.NullInt64{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(int(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(int8(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(int16(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(int32(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(int64(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(uint(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(uint8(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(uint16(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(uint32(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr(uint64(123))},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullByte{Valid: false}},
			want: Want{value: sql.NullInt64{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullInt16{Valid: false}},
			want: Want{value: sql.NullInt64{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullInt32{Valid: false}},
			want: Want{value: sql.NullInt64{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullInt64{Valid: false}},
			want: Want{value: sql.NullInt64{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullByte{Valid: true, Byte: 123}},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullInt16{Valid: true, Int16: 123}},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullInt32{Valid: true, Int32: 123}},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: sql.NullInt64{Valid: true, Int64: 123}},
			want: Want{value: sql.NullInt64{Valid: true, Int64: 123}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: struct{}{}},
			want: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		got, ok := testCase.sut.ToNullInt64()

		if ok != testCase.want.ok {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, ok, testCase.want.ok)
		}
		if ok && got != testCase.want.value {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, got, testCase.want.value)
		}
	}
}

func TestValue_ToNullBytes(t *testing.T) {
	type Want struct {
		value dbsql.NullBytes
		ok    bool
	}
	type TestCase struct {
		sut  *dbsql.Value
		want Want
	}
	testCases := []TestCase{
		{
			sut:  &dbsql.Value{Any: nil},
			want: Want{value: dbsql.NullBytes{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: []byte("abc")},
			want: Want{value: dbsql.NullBytes{Valid: true, Bytes: []byte("abc")}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ([]byte)(nil)},
			want: Want{value: dbsql.NullBytes{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: (*[]byte)(nil)},
			want: Want{value: dbsql.NullBytes{Valid: false}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: ptr([]byte("abc"))},
			want: Want{value: dbsql.NullBytes{Valid: true, Bytes: []byte("abc")}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: dbsql.NullBytes{Valid: false}},
			want: Want{value: dbsql.NullBytes{}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: dbsql.NullBytes{Valid: true, Bytes: []byte("abc")}},
			want: Want{value: dbsql.NullBytes{Valid: true, Bytes: []byte("abc")}, ok: true},
		},
		{
			sut:  &dbsql.Value{Any: struct{}{}},
			want: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		got, ok := testCase.sut.ToNullBytes()

		if ok != testCase.want.ok {
			t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, ok, testCase.want.ok)
		}
		if ok {
			if got.Valid != testCase.want.value.Valid {
				t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, got, testCase.want.value)
			}
			if string(got.Bytes) != string(testCase.want.value.Bytes) {
				t.Fatalf("sut[%d] = %#v\n  got %#v\n  want %#v", i, testCase.sut, got, testCase.want.value)
			}
		}
	}
}

func equalsVal[T comparable](gotVal T, gotOK bool, wantVal any, wantOK bool) bool {
	if gotOK != wantOK {
		return false
	}
	if gotOK {
		wantVal, ok := wantVal.(T)
		if !ok {
			return false
		}
		if gotVal != wantVal {
			return false
		}
	}
	return true
}

func equalsValPtr[T comparable](gotValPtr *T, gotOK bool, wantValPtr any, wantOK bool) bool {
	if gotOK != wantOK {
		return false
	}
	if gotOK {
		if (gotValPtr == nil) != (wantValPtr == (*T)(nil)) {
			return false
		}
		if gotValPtr != nil {
			wantValPtr, ok := wantValPtr.(*T)
			if !ok {
				return false
			}
			if *gotValPtr != *wantValPtr {
				return false
			}
		}
	}
	return true
}

func TestValue_AssignTo_String(t *testing.T) {
	type Want struct {
		value any
		ok    bool
	}
	type TestCase struct {
		sut         *dbsql.Value
		wantVal     Want
		wantValPtr  Want
		wantNullVal Want
	}
	testCases := []TestCase{
		{
			sut:         &dbsql.Value{Any: nil},
			wantVal:     Want{value: "", ok: false},
			wantValPtr:  Want{value: (*string)(nil), ok: true},
			wantNullVal: Want{value: sql.NullString{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: "abc"},
			wantVal:     Want{value: "abc", ok: true},
			wantValPtr:  Want{value: ptr("abc"), ok: true},
			wantNullVal: Want{value: sql.NullString{Valid: true, String: "abc"}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: (*string)(nil)},
			wantVal:     Want{value: "", ok: false},
			wantValPtr:  Want{value: (*string)(nil), ok: true},
			wantNullVal: Want{value: sql.NullString{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: ptr("abc")},
			wantVal:     Want{value: "abc", ok: true},
			wantValPtr:  Want{value: ptr("abc"), ok: true},
			wantNullVal: Want{value: sql.NullString{Valid: true, String: "abc"}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: sql.NullString{Valid: false}},
			wantVal:     Want{value: "", ok: false},
			wantValPtr:  Want{value: (*string)(nil), ok: true},
			wantNullVal: Want{value: sql.NullString{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: sql.NullString{Valid: true, String: "abc"}},
			wantVal:     Want{value: "abc", ok: true},
			wantValPtr:  Want{value: ptr("abc"), ok: true},
			wantNullVal: Want{value: sql.NullString{Valid: true, String: "abc"}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: struct{}{}},
			wantVal:     Want{ok: false},
			wantValPtr:  Want{ok: false},
			wantNullVal: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		{
			var got string
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantVal.value, testCase.wantVal.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal)
			}
		}
		{
			var got *string
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wantValPtr.value, testCase.wantValPtr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantValPtr)
			}
		}
		{
			var got sql.NullString
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantNullVal.value, testCase.wantNullVal.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantNullVal)
			}
		}
	}
}

func TestValue_AssignTo_Bool(t *testing.T) {
	type Want struct {
		value any
		ok    bool
	}
	type TestCase struct {
		sut         *dbsql.Value
		wantVal     Want
		wantValPtr  Want
		wantNullVal Want
	}
	testCases := []TestCase{
		{
			sut:         &dbsql.Value{Any: nil},
			wantVal:     Want{value: false, ok: false},
			wantValPtr:  Want{value: (*bool)(nil), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: true},
			wantVal:     Want{value: true, ok: true},
			wantValPtr:  Want{value: ptr(true), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: true, Bool: true}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: false},
			wantVal:     Want{value: false, ok: true},
			wantValPtr:  Want{value: ptr(false), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: true, Bool: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: (*bool)(nil)},
			wantVal:     Want{value: false, ok: false},
			wantValPtr:  Want{value: (*bool)(nil), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: ptr(true)},
			wantVal:     Want{value: true, ok: true},
			wantValPtr:  Want{value: ptr(true), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: true, Bool: true}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: ptr(false)},
			wantVal:     Want{value: false, ok: true},
			wantValPtr:  Want{value: ptr(false), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: true, Bool: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: sql.NullBool{Valid: false}},
			wantVal:     Want{value: "", ok: false},
			wantValPtr:  Want{value: (*bool)(nil), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: sql.NullBool{Valid: true, Bool: true}},
			wantVal:     Want{value: true, ok: true},
			wantValPtr:  Want{value: ptr(true), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: true, Bool: true}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: sql.NullBool{Valid: true, Bool: false}},
			wantVal:     Want{value: false, ok: true},
			wantValPtr:  Want{value: ptr(false), ok: true},
			wantNullVal: Want{value: sql.NullBool{Valid: true, Bool: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: struct{}{}},
			wantVal:     Want{ok: false},
			wantValPtr:  Want{ok: false},
			wantNullVal: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		{
			var got bool
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantVal.value, testCase.wantVal.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal)
			}
		}
		{
			var got *bool
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wantValPtr.value, testCase.wantValPtr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantValPtr)
			}
		}
		{
			var got sql.NullBool
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantNullVal.value, testCase.wantNullVal.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantNullVal)
			}
		}
	}
}

func TestValue_AssignTo_Float64(t *testing.T) {
	type Want struct {
		value any
		ok    bool
	}
	type TestCase struct {
		sut          *dbsql.Value
		wantVal32    Want
		wantVal64    Want
		wantVal32Ptr Want
		wantVal64Ptr Want
		wantNullVal  Want
	}
	testCases := []TestCase{
		{
			sut:          &dbsql.Value{Any: nil},
			wantVal32:    Want{value: float32(0), ok: false},
			wantVal64:    Want{value: float64(0), ok: false},
			wantVal32Ptr: Want{value: (*float32)(nil), ok: true},
			wantVal64Ptr: Want{value: (*float64)(nil), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: false}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: float32(-0.5)},
			wantVal32:    Want{value: float32(-0.5), ok: true},
			wantVal64:    Want{value: float64(-0.5), ok: true},
			wantVal32Ptr: Want{value: ptr(float32(-0.5)), ok: true},
			wantVal64Ptr: Want{value: ptr(float64(-0.5)), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: float64(-0.5)},
			wantVal32:    Want{value: float32(-0.5), ok: true},
			wantVal64:    Want{value: float64(-0.5), ok: true},
			wantVal32Ptr: Want{value: ptr(float32(-0.5)), ok: true},
			wantVal64Ptr: Want{value: ptr(float64(-0.5)), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: (*float32)(nil)},
			wantVal32:    Want{value: float32(0), ok: false},
			wantVal64:    Want{value: float64(0), ok: false},
			wantVal32Ptr: Want{value: (*float32)(nil), ok: true},
			wantVal64Ptr: Want{value: (*float64)(nil), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: false}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: (*float64)(nil)},
			wantVal32:    Want{value: float32(0), ok: false},
			wantVal64:    Want{value: float64(0), ok: false},
			wantVal32Ptr: Want{value: (*float32)(nil), ok: true},
			wantVal64Ptr: Want{value: (*float64)(nil), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: false}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: ptr(float32(-0.5))},
			wantVal32:    Want{value: float32(-0.5), ok: true},
			wantVal64:    Want{value: float64(-0.5), ok: true},
			wantVal32Ptr: Want{value: ptr(float32(-0.5)), ok: true},
			wantVal64Ptr: Want{value: ptr(float64(-0.5)), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: ptr(float64(-0.5))},
			wantVal32:    Want{value: float32(-0.5), ok: true},
			wantVal64:    Want{value: float64(-0.5), ok: true},
			wantVal32Ptr: Want{value: ptr(float32(-0.5)), ok: true},
			wantVal64Ptr: Want{value: ptr(float64(-0.5)), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: sql.NullFloat64{Valid: false}},
			wantVal32:    Want{value: float32(0), ok: false},
			wantVal64:    Want{value: float64(0), ok: false},
			wantVal32Ptr: Want{value: (*float32)(nil), ok: true},
			wantVal64Ptr: Want{value: (*float64)(nil), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: false}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: sql.NullFloat64{Valid: true, Float64: -0.5}},
			wantVal32:    Want{value: float32(-0.5), ok: true},
			wantVal64:    Want{value: float64(-0.5), ok: true},
			wantVal32Ptr: Want{value: ptr(float32(-0.5)), ok: true},
			wantVal64Ptr: Want{value: ptr(float64(-0.5)), ok: true},
			wantNullVal:  Want{value: sql.NullFloat64{Valid: true, Float64: -0.5}, ok: true},
		},
		{
			sut:          &dbsql.Value{Any: struct{}{}},
			wantVal32:    Want{ok: false},
			wantVal64:    Want{ok: false},
			wantVal32Ptr: Want{ok: false},
			wantVal64Ptr: Want{ok: false},
			wantNullVal:  Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		{
			var got float32
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantVal32.value, testCase.wantVal32.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal32)
			}
		}
		{
			var got float64
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantVal64.value, testCase.wantVal64.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal64)
			}
		}
		{
			var got *float32
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wantVal32Ptr.value, testCase.wantVal32Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal32Ptr)
			}
		}
		{
			var got *float64
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wantVal64Ptr.value, testCase.wantVal64Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal64Ptr)
			}
		}
		{
			var got sql.NullFloat64
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantNullVal.value, testCase.wantNullVal.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantNullVal)
			}
		}
	}
}

func TestValue_AssignTo_Int64(t *testing.T) {
	type Want struct {
		value any
		ok    bool
	}
	type Wants struct {
		val         Want
		val8        Want
		val16       Want
		val32       Want
		val64       Want
		valU        Want
		valU8       Want
		valU16      Want
		valU32      Want
		valU64      Want
		valPtr      Want
		val8Ptr     Want
		val16Ptr    Want
		val32Ptr    Want
		val64Ptr    Want
		valUPtr     Want
		valU8Ptr    Want
		valU16Ptr   Want
		valU32Ptr   Want
		valU64Ptr   Want
		nullValByte Want
		nullVal16   Want
		nullVal32   Want
		nullVal64   Want
	}
	type TestCase struct {
		sut   *dbsql.Value
		wants Wants
	}
	wantsNull := Wants{
		val:         Want{ok: false},
		val8:        Want{ok: false},
		val16:       Want{ok: false},
		val32:       Want{ok: false},
		val64:       Want{ok: false},
		valU:        Want{ok: false},
		valU8:       Want{ok: false},
		valU16:      Want{ok: false},
		valU32:      Want{ok: false},
		valU64:      Want{ok: false},
		valPtr:      Want{ok: true, value: (*int)(nil)},
		val8Ptr:     Want{ok: true, value: (*int8)(nil)},
		val16Ptr:    Want{ok: true, value: (*int16)(nil)},
		val32Ptr:    Want{ok: true, value: (*int32)(nil)},
		val64Ptr:    Want{ok: true, value: (*int64)(nil)},
		valUPtr:     Want{ok: true, value: (*uint)(nil)},
		valU8Ptr:    Want{ok: true, value: (*uint8)(nil)},
		valU16Ptr:   Want{ok: true, value: (*uint16)(nil)},
		valU32Ptr:   Want{ok: true, value: (*uint32)(nil)},
		valU64Ptr:   Want{ok: true, value: (*uint64)(nil)},
		nullValByte: Want{ok: true, value: sql.NullByte{}},
		nullVal16:   Want{ok: true, value: sql.NullInt16{}},
		nullVal32:   Want{ok: true, value: sql.NullInt32{}},
		nullVal64:   Want{ok: true, value: sql.NullInt64{}},
	}
	wantsNonNull := Wants{
		val:         Want{ok: true, value: int(123)},
		val8:        Want{ok: true, value: int8(123)},
		val16:       Want{ok: true, value: int16(123)},
		val32:       Want{ok: true, value: int32(123)},
		val64:       Want{ok: true, value: int64(123)},
		valU:        Want{ok: true, value: uint(123)},
		valU8:       Want{ok: true, value: uint8(123)},
		valU16:      Want{ok: true, value: uint16(123)},
		valU32:      Want{ok: true, value: uint32(123)},
		valU64:      Want{ok: true, value: uint64(123)},
		valPtr:      Want{ok: true, value: ptr(int(123))},
		val8Ptr:     Want{ok: true, value: ptr(int8(123))},
		val16Ptr:    Want{ok: true, value: ptr(int16(123))},
		val32Ptr:    Want{ok: true, value: ptr(int32(123))},
		val64Ptr:    Want{ok: true, value: ptr(int64(123))},
		valUPtr:     Want{ok: true, value: ptr(uint(123))},
		valU8Ptr:    Want{ok: true, value: ptr(uint8(123))},
		valU16Ptr:   Want{ok: true, value: ptr(uint16(123))},
		valU32Ptr:   Want{ok: true, value: ptr(uint32(123))},
		valU64Ptr:   Want{ok: true, value: ptr(uint64(123))},
		nullValByte: Want{ok: true, value: sql.NullByte{Valid: true, Byte: byte(123)}},
		nullVal16:   Want{ok: true, value: sql.NullInt16{Valid: true, Int16: int16(123)}},
		nullVal32:   Want{ok: true, value: sql.NullInt32{Valid: true, Int32: int32(123)}},
		nullVal64:   Want{ok: true, value: sql.NullInt64{Valid: true, Int64: int64(123)}},
	}
	testCases := []TestCase{
		{
			sut:   &dbsql.Value{Any: nil},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: int(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: int8(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: int16(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: int32(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: int64(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: uint(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: uint8(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: uint16(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: uint32(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: uint64(123)},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: (*int)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*int8)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*int16)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*int32)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*int64)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*uint)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*uint8)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*uint16)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*uint32)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: (*uint64)(nil)},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(int(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(int8(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(int16(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(int32(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(int64(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(uint(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(uint8(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(uint16(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(uint32(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: ptr(uint64(123))},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: sql.NullByte{Valid: false}},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: sql.NullInt16{Valid: false}},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: sql.NullInt32{Valid: false}},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: sql.NullInt64{Valid: false}},
			wants: wantsNull,
		},
		{
			sut:   &dbsql.Value{Any: sql.NullByte{Valid: true, Byte: byte(123)}},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: sql.NullInt16{Valid: true, Int16: int16(123)}},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: sql.NullInt32{Valid: true, Int32: int32(123)}},
			wants: wantsNonNull,
		},
		{
			sut:   &dbsql.Value{Any: sql.NullInt64{Valid: true, Int64: int64(123)}},
			wants: wantsNonNull,
		},
		{
			sut: &dbsql.Value{Any: struct{}{}},
			wants: Wants{
				val:         Want{ok: false},
				val8:        Want{ok: false},
				val16:       Want{ok: false},
				val32:       Want{ok: false},
				val64:       Want{ok: false},
				valU:        Want{ok: false},
				valU8:       Want{ok: false},
				valU16:      Want{ok: false},
				valU32:      Want{ok: false},
				valU64:      Want{ok: false},
				valPtr:      Want{ok: false},
				val8Ptr:     Want{ok: false},
				val16Ptr:    Want{ok: false},
				val32Ptr:    Want{ok: false},
				val64Ptr:    Want{ok: false},
				valUPtr:     Want{ok: false},
				valU8Ptr:    Want{ok: false},
				valU16Ptr:   Want{ok: false},
				valU32Ptr:   Want{ok: false},
				valU64Ptr:   Want{ok: false},
				nullValByte: Want{ok: false},
				nullVal16:   Want{ok: false},
				nullVal32:   Want{ok: false},
				nullVal64:   Want{ok: false},
			},
		},
	}

	for i, testCase := range testCases {
		{
			var got int
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.val.value, testCase.wants.val.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val)
			}
		}
		{
			var got int8
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.val8.value, testCase.wants.val8.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val8)
			}
		}
		{
			var got int16
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.val16.value, testCase.wants.val16.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val16)
			}
		}
		{
			var got int32
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.val32.value, testCase.wants.val32.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val32)
			}
		}
		{
			var got int64
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.val64.value, testCase.wants.val64.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val64)
			}
		}
		{
			var got uint
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.valU.value, testCase.wants.valU.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU)
			}
		}
		{
			var got uint8
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.valU8.value, testCase.wants.valU8.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU8)
			}
		}
		{
			var got uint16
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.valU16.value, testCase.wants.valU16.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU16)
			}
		}
		{
			var got uint32
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.valU32.value, testCase.wants.valU32.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU32)
			}
		}
		{
			var got uint64
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.valU64.value, testCase.wants.valU64.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU64)
			}
		}
		{
			var got *int
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.valPtr.value, testCase.wants.valPtr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valPtr)
			}
		}
		{
			var got *int8
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.val8Ptr.value, testCase.wants.val8Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val8Ptr)
			}
		}
		{
			var got *int16
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.val16Ptr.value, testCase.wants.val16Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val16Ptr)
			}
		}
		{
			var got *int32
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.val32Ptr.value, testCase.wants.val32Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val32Ptr)
			}
		}
		{
			var got *int64
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.val64Ptr.value, testCase.wants.val64Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.val64Ptr)
			}
		}
		{
			var got *uint
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.valUPtr.value, testCase.wants.valUPtr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valUPtr)
			}
		}
		{
			var got *uint8
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.valU8Ptr.value, testCase.wants.valU8Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU8Ptr)
			}
		}
		{
			var got *uint16
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.valU16Ptr.value, testCase.wants.valU16Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU16Ptr)
			}
		}
		{
			var got *uint32
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.valU32Ptr.value, testCase.wants.valU32Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU32Ptr)
			}
		}
		{
			var got *uint64
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wants.valU64Ptr.value, testCase.wants.valU64Ptr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.valU64Ptr)
			}
		}
		{
			var got sql.NullByte
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.nullValByte.value, testCase.wants.nullValByte.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.nullValByte)
			}
		}
		{
			var got sql.NullInt16
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.nullVal16.value, testCase.wants.nullVal16.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.nullVal16)
			}
		}
		{
			var got sql.NullInt32
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.nullVal32.value, testCase.wants.nullVal32.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.nullVal32)
			}
		}
		{
			var got sql.NullInt64
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wants.nullVal64.value, testCase.wants.nullVal64.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wants.nullVal64)
			}
		}
	}
}

func TestValue_AssignTo_Time(t *testing.T) {
	type Want struct {
		value any
		ok    bool
	}
	type TestCase struct {
		sut         *dbsql.Value
		wantVal     Want
		wantValPtr  Want
		wantNullVal Want
	}
	date := time.Date(2023, 5, 1, 1, 2, 3, 4, time.Local)
	testCases := []TestCase{
		{
			sut:         &dbsql.Value{Any: nil},
			wantVal:     Want{ok: false},
			wantValPtr:  Want{value: (*time.Time)(nil), ok: true},
			wantNullVal: Want{value: sql.NullTime{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: date},
			wantVal:     Want{value: date, ok: true},
			wantValPtr:  Want{value: ptr(date), ok: true},
			wantNullVal: Want{value: sql.NullTime{Valid: true, Time: date}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: (*time.Time)(nil)},
			wantVal:     Want{ok: false},
			wantValPtr:  Want{value: (*time.Time)(nil), ok: true},
			wantNullVal: Want{value: sql.NullTime{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: ptr(date)},
			wantVal:     Want{value: date, ok: true},
			wantValPtr:  Want{value: ptr(date), ok: true},
			wantNullVal: Want{value: sql.NullTime{Valid: true, Time: date}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: sql.NullTime{Valid: false}},
			wantVal:     Want{ok: false},
			wantValPtr:  Want{value: (*time.Time)(nil), ok: true},
			wantNullVal: Want{value: sql.NullTime{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: sql.NullTime{Valid: true, Time: date}},
			wantVal:     Want{value: date, ok: true},
			wantValPtr:  Want{value: ptr(date), ok: true},
			wantNullVal: Want{value: sql.NullTime{Valid: true, Time: date}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: struct{}{}},
			wantVal:     Want{ok: false},
			wantValPtr:  Want{ok: false},
			wantNullVal: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		{
			var got time.Time
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantVal.value, testCase.wantVal.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal)
			}
		}
		{
			var got *time.Time
			ok := testCase.sut.AssignTo(&got)
			if !equalsValPtr(got, ok, testCase.wantValPtr.value, testCase.wantValPtr.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantValPtr)
			}
		}
		{
			var got sql.NullTime
			ok := testCase.sut.AssignTo(&got)
			if !equalsVal(got, ok, testCase.wantNullVal.value, testCase.wantNullVal.ok) {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantNullVal)
			}
		}
	}
}

func TestValue_AssignTo_Bytes(t *testing.T) {
	type Want struct {
		value any
		ok    bool
	}
	type TestCase struct {
		sut         *dbsql.Value
		wantVal     Want
		wantValPtr  Want
		wantNullVal Want
	}

	testCases := []TestCase{
		{
			sut:         &dbsql.Value{Any: nil},
			wantVal:     Want{value: []byte(nil), ok: true},
			wantValPtr:  Want{value: (*[]byte)(nil), ok: true},
			wantNullVal: Want{value: dbsql.NullBytes{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: []byte("abc")},
			wantVal:     Want{value: []byte("abc"), ok: true},
			wantValPtr:  Want{value: ptr([]byte("abc")), ok: true},
			wantNullVal: Want{value: dbsql.NullBytes{Valid: true, Bytes: []byte("abc")}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: ([]byte)(nil)},
			wantVal:     Want{value: []byte(nil), ok: true},
			wantValPtr:  Want{value: (*[]byte)(nil), ok: true},
			wantNullVal: Want{value: dbsql.NullBytes{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: (*[]byte)(nil)},
			wantVal:     Want{value: []byte(nil), ok: true},
			wantValPtr:  Want{value: (*[]byte)(nil), ok: true},
			wantNullVal: Want{value: dbsql.NullBytes{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: ptr([]byte("abc"))},
			wantVal:     Want{value: []byte("abc"), ok: true},
			wantValPtr:  Want{value: ptr([]byte("abc")), ok: true},
			wantNullVal: Want{value: dbsql.NullBytes{Valid: true, Bytes: []byte("abc")}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: dbsql.NullBytes{Valid: false}},
			wantVal:     Want{value: []byte(nil), ok: true},
			wantValPtr:  Want{value: (*[]byte)(nil), ok: true},
			wantNullVal: Want{value: dbsql.NullBytes{Valid: false}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: dbsql.NullBytes{Valid: true, Bytes: []byte("abc")}},
			wantVal:     Want{value: []byte("abc"), ok: true},
			wantValPtr:  Want{value: ptr([]byte("abc")), ok: true},
			wantNullVal: Want{value: dbsql.NullBytes{Valid: true, Bytes: []byte("abc")}, ok: true},
		},
		{
			sut:         &dbsql.Value{Any: struct{}{}},
			wantVal:     Want{ok: false},
			wantValPtr:  Want{ok: false},
			wantNullVal: Want{ok: false},
		},
	}

	for i, testCase := range testCases {
		{
			var got []byte
			ok := testCase.sut.AssignTo(&got)
			if ok != testCase.wantVal.ok {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal)
			}
			if ok {
				wantVal, ok := testCase.wantVal.value.([]byte)
				if !ok {
					t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal)
				}
				if string(got) != string(wantVal) {
					t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantVal)
				}
			}
		}
		{
			var got *[]byte
			ok := testCase.sut.AssignTo(&got)
			if ok != testCase.wantVal.ok {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantValPtr)
			}
			if ok {
				if (got == nil) != (testCase.wantValPtr.value == (*[]byte)(nil)) {
					t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantValPtr)
				}
				if got != nil {
					wantValPtr, ok := testCase.wantValPtr.value.(*[]byte)
					if !ok {
						t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantValPtr)
					}
					if string(*got) != string(*wantValPtr) {
						t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantValPtr)
					}
				}
			}
		}
		{
			var got dbsql.NullBytes
			ok := testCase.sut.AssignTo(&got)
			if ok != testCase.wantNullVal.ok {
				t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantNullVal)
			}
			if ok {
				wantVal, ok := testCase.wantNullVal.value.(dbsql.NullBytes)
				if !ok {
					t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantNullVal)
				}
				if got.Valid != wantVal.Valid {
					t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantNullVal)
				}
				if string(got.Bytes) != string(wantVal.Bytes) {
					t.Fatalf("sut[%d] = %#v\n  got %#v, ok %#v\n  want %#v", i, testCase.sut, got, ok, testCase.wantNullVal)
				}
			}
		}
	}
}
