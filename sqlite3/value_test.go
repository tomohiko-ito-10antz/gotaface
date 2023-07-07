package sqlite3_test

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/Jumpaku/gotaface/sqlite3"
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
