package schema_test

import (
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	spanner_impl "github.com/Jumpaku/gotaface/spanner"
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
