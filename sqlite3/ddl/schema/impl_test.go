package schema_test

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/Jumpaku/gotaface/sqlite3/ddl/schema"
)

func TestGoType(t *testing.T) {
	type testCase struct {
		input schema.Column
		want  reflect.Type
	}
	testCases := []testCase{
		{
			input: schema.Column{TypeVal: "INT"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "INTEGER"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "TINYINT"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "SMALLINT"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "MEDIUMINT"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "BIGINT"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "UNSIGNED BIG INT"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "INT2"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "INT8"},
			want:  schema.RefType[sql.NullInt64](),
		}, {
			input: schema.Column{TypeVal: "CHARACTER(20)"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "VARCHAR(255)"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "VARYING CHARACTER(255)"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "NCHAR(55)"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "NATIVE CHARACTER(70)"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "NVARCHAR(100)"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "TEXT"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "CLOB"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "BLOB"},
			want:  schema.RefType[[]byte](),
		}, {
			input: schema.Column{TypeVal: ""},
			want:  schema.RefType[[]byte](),
		}, {
			input: schema.Column{TypeVal: "REAL"},
			want:  schema.RefType[sql.NullFloat64](),
		}, {
			input: schema.Column{TypeVal: "DOUBLE"},
			want:  schema.RefType[sql.NullFloat64](),
		}, {
			input: schema.Column{TypeVal: "DOUBLE PRECISION"},
			want:  schema.RefType[sql.NullFloat64](),
		}, {
			input: schema.Column{TypeVal: "FLOAT"},
			want:  schema.RefType[sql.NullFloat64](),
		}, {
			input: schema.Column{TypeVal: "NUMERIC"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "DECIMAL(10,5)"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "BOOLEAN"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "DATE"},
			want:  schema.RefType[sql.NullString](),
		}, {
			input: schema.Column{TypeVal: "DATETIME"},
			want:  schema.RefType[sql.NullString](),
		},
	}

	for _, testCase := range testCases {
		got := schema.GoType(testCase.input)
		if got.Name() != testCase.want.Name() {
			t.Errorf("GoType() mismatch\n  got  = %v\n  want = %v", got, testCase.want)
		}
	}
}
