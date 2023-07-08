package sqlite3

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func RefType[T any]() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}
func GoType(columnType string) reflect.Type {
	lower := strings.ToLower(columnType)
	switch {
	case strings.Contains(lower, "int"):
		return RefType[sql.NullInt64]()
	case strings.Contains(lower, "char"), strings.Contains(lower, "clob"), strings.Contains(lower, "text"):
		return RefType[sql.NullString]()
	case strings.Contains(lower, "blob"), lower == "":
		return RefType[[]byte]()
	case strings.Contains(lower, "real"), strings.Contains(lower, "floa"), strings.Contains(lower, "doub"):
		return RefType[sql.NullFloat64]()
	default:
		return RefType[sql.NullString]()
	}
}

func mustInt64(v any) int64 {
	switch v := v.(type) {
	default:
		panic(`cannot convert to int64`)
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	}
}
func ToDBValue(columnType string, src any) (any, error) {
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || (rv.Kind() == reflect.Pointer && rv.IsNil()) {
		return reflect.Zero(GoType(columnType)).Interface(), nil
	}
	if rv.Kind() == reflect.Pointer {
		return ToDBValue(columnType, rv.Elem().Interface())
	}

	lower := strings.ToLower(columnType)
	switch {
	default:
		return ToDBValue("text", src)
	case strings.Contains(lower, "int"):
		switch src := src.(type) {
		case json.Number:
			dst, err := src.Int64()
			if err != nil {
				return nil, fmt.Errorf(`fail to convert %v as NullInt64: %w`, spew.Sdump(src), err)
			}
			return sql.NullInt64{Valid: true, Int64: dst}, nil
		case int, int8, int16, int32, uint, uint8, uint16, uint32, uint64:
			return ToDBValue(columnType, mustInt64(src))
		default:
			dst := &sql.NullInt64{}
			if err := dst.Scan(src); err != nil {
				return nil, fmt.Errorf(`fail to scan %v as NullInt64: %w`, spew.Sdump(src), err)
			}
			return *dst, nil
		}
	case strings.Contains(lower, "char"), strings.Contains(lower, "clob"), strings.Contains(lower, "text"):
		dst := &sql.NullString{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan %v as NullString: %w`, spew.Sdump(src), err)
		}
		return *dst, nil
	case strings.Contains(lower, "blob"), lower == "":
		switch src := src.(type) {
		default:
			return nil, fmt.Errorf(`fail to convert %v as bytes: %#v`, spew.Sdump(src), src)
		case []byte:
			return src, nil
		case string:
			jsonStringBytes, err := protojson.Marshal(wrapperspb.String(src))
			if err != nil {
				return nil, fmt.Errorf(`fail to marshal %v to bytes: %w`, spew.Sdump(src), err)
			}
			dst := &wrappers.BytesValue{}
			if err := protojson.Unmarshal(jsonStringBytes, dst); err != nil {
				return nil, fmt.Errorf(`fail to decode bytes from %v base64-encoded: %w`, spew.Sdump(src), err)
			}
			return dst.Value, nil
		}
	case strings.Contains(lower, "real"), strings.Contains(lower, "floa"), strings.Contains(lower, "doub"):
		switch src := src.(type) {
		case json.Number:
			dst, err := src.Float64()
			if err != nil {
				return nil, fmt.Errorf(`fail to convert %v as NullFloat64: %w`, spew.Sdump(src), err)
			}
			return sql.NullFloat64{Valid: true, Float64: dst}, nil
		case float32:
			return ToDBValue(columnType, float64(src))
		default:
			dst := &sql.NullFloat64{}
			if err := dst.Scan(src); err != nil {
				return nil, fmt.Errorf(`fail to scan %v as NullFloat64: %w`, spew.Sdump(src), err)
			}
			return *dst, nil
		}
	}
}
