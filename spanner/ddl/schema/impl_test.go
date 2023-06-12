package schema_test

import (
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/spanner/ddl/schema"
)

func TestGoType(t *testing.T) {
	type testCase struct {
		input schema.Column
		want  reflect.Type
	}
	testCases := []testCase{
		{
			input: schema.Column{TypeVal: "INT64"},
			want:  schema.RefType[spanner.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "STRING(MAX)"},
			want:  schema.RefType[spanner.NullString](),
		}, {
			input: schema.Column{TypeVal: "BOOL"},
			want:  schema.RefType[spanner.NullBool](),
		}, {
			input: schema.Column{TypeVal: "FLOAT64"},
			want:  schema.RefType[spanner.NullFloat64](),
		}, {
			input: schema.Column{TypeVal: "TIMESTAMP"},
			want:  schema.RefType[spanner.NullTime](),
		}, {
			input: schema.Column{TypeVal: "DATE"},
			want:  schema.RefType[spanner.NullDate](),
		}, {
			input: schema.Column{TypeVal: "NUMERIC"},
			want:  schema.RefType[spanner.NullNumeric](),
		}, {
			input: schema.Column{TypeVal: "BYTES(255)"},
			want:  schema.RefType[[]byte](),
		}, {
			input: schema.Column{TypeVal: "JSON"},
			want:  schema.RefType[spanner.NullJSON](),
		}, {
			input: schema.Column{TypeVal: "ARRAY<INT64>"},
			want:  schema.RefType[[]spanner.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "ARRAY<STRUCT<INT64>>"},
			want:  schema.RefType[[]spanner.NullRow](),
		}, {
			input: schema.Column{TypeVal: "STRUCT<x INT64, y STRING(12)>"},
			want:  schema.RefType[spanner.NullRow](),
		}, {
			input: schema.Column{TypeVal: "STRUCT<ARRAY<INT64>>"},
			want:  schema.RefType[spanner.NullRow](),
		},
	}

	for _, testCase := range testCases {
		got := schema.GoType(testCase.input)
		if got.Name() != testCase.want.Name() {
			t.Errorf("GoType() mismatch\n  got  = %v\n  want = %v", got, testCase.want)
		}
	}
}
