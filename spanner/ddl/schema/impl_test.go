package schema_test

import (
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/spanner/ddl/schema"
)

func refType[T any]() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}
func TestGoType(t *testing.T) {
	type testCase struct {
		input schema.Column
		want  reflect.Type
	}
	testCases := []testCase{
		{
			input: schema.Column{TypeVal: "INT64"},
			want:  refType[spanner.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "STRING(MAX)"},
			want:  refType[spanner.NullString](),
		}, {
			input: schema.Column{TypeVal: "BOOL"},
			want:  refType[spanner.NullBool](),
		}, {
			input: schema.Column{TypeVal: "FLOAT64"},
			want:  refType[spanner.NullFloat64](),
		}, {
			input: schema.Column{TypeVal: "TIMESTAMP"},
			want:  refType[spanner.NullTime](),
		}, {
			input: schema.Column{TypeVal: "DATE"},
			want:  refType[spanner.NullDate](),
		}, {
			input: schema.Column{TypeVal: "NUMERIC"},
			want:  refType[spanner.NullNumeric](),
		}, {
			input: schema.Column{TypeVal: "BYTES(255)"},
			want:  refType[[]byte](),
		}, {
			input: schema.Column{TypeVal: "JSON"},
			want:  refType[spanner.NullJSON](),
		}, {
			input: schema.Column{TypeVal: "ARRAY<INT64>"},
			want:  refType[[]spanner.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "ARRAY<STRUCT<INT64>>"},
			want:  refType[[]spanner.NullRow](),
		}, {
			input: schema.Column{TypeVal: "STRUCT<x INT64, y STRING(12)>"},
			want:  refType[spanner.NullRow](),
		}, {
			input: schema.Column{TypeVal: "STRUCT<ARRAY<INT64>>"},
			want:  refType[spanner.NullRow](),
		},
	}

	for _, testCase := range testCases {
		got := schema.GoType(testCase.input)
		if got.Name() != testCase.want.Name() {
			t.Errorf("GoType() mismatch\n  got  = %v\n  want = %v", got, testCase.want)
		}
	}
}
