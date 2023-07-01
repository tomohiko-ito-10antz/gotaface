package spanner

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/spanner"
)

func RefType[T any]() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}

func GoType(columnType string) reflect.Type {
	lower := strings.ToLower(columnType)
	switch {
	case strings.HasPrefix(lower, "int64"):
		return RefType[spanner.NullInt64]()
	case strings.HasPrefix(lower, "string"):
		return RefType[spanner.NullString]()
	case strings.HasPrefix(lower, "bool"):
		return RefType[spanner.NullBool]()
	case strings.HasPrefix(lower, "float64"):
		return RefType[spanner.NullFloat64]()
	case strings.HasPrefix(lower, "timestamp"):
		return RefType[spanner.NullTime]()
	case strings.HasPrefix(lower, "date"):
		return RefType[spanner.NullDate]()
	case strings.HasPrefix(lower, "numeric"):
		return RefType[spanner.NullNumeric]()
	case strings.HasPrefix(lower, "bytes"):
		return RefType[[]byte]()
	case strings.HasPrefix(lower, "json"):
		return RefType[spanner.NullJSON]()
	case strings.HasPrefix(lower, "array<"):
		return reflect.SliceOf(GoType(lower[6 : len(lower)-1]))
	case strings.HasPrefix(lower, "struct"):
		return RefType[spanner.NullRow]()
	default:
		return RefType[spanner.GenericColumnValue]()
	}
}

func ConvertValue(columnType string, src any) (any, error) {
	lower := strings.ToLower(columnType)
	switch {
	default:
		return nil, fmt.Errorf(`unsupported column type: %v`, columnType)
	case strings.HasPrefix(lower, "int64"):
		dst := &spanner.NullInt64{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan value as NullInt64: %w`, err)
		}
		return dst, nil
	case strings.HasPrefix(lower, "string"):
		dst := &spanner.NullString{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan value as NullString: %w`, err)
		}
		return dst, nil
	case strings.HasPrefix(lower, "bool"):
		dst := &spanner.NullBool{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan value as NullBool: %w`, err)
		}
		return dst, nil
	case strings.HasPrefix(lower, "float64"):
		dst := &spanner.NullFloat64{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan value as NullFloat64: %w`, err)
		}
		return dst, nil
	case strings.HasPrefix(lower, "timestamp"):
		dst := &spanner.NullTime{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan value as NullTime: %w`, err)
		}
		return dst, nil
	case strings.HasPrefix(lower, "date"):
		dst := &spanner.NullDate{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan value as NullDate: %w`, err)
		}
		return dst, nil
	case strings.HasPrefix(lower, "numeric"):
		dst := &spanner.NullNumeric{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan value as NullNumeric: %w`, err)
		}
		return dst, nil
	case strings.HasPrefix(lower, "bytes"):
		switch src := src.(type) {
		default:
			return nil, fmt.Errorf(`fail to convert value as bytes: %#v`, src)
		case nil:
			return (*[]byte)(nil), nil
		case *[]byte:
			if src == nil {
				return (*[]byte)(nil), nil
			}
			return ConvertValue(columnType, *src)
		case *string:
			if src == nil {
				return (*[]byte)(nil), nil
			}
			return ConvertValue(columnType, *src)
		case []byte:
			return src, nil
		case string:
			dst, err := base64.StdEncoding.DecodeString(src)
			if err != nil {
				return nil, fmt.Errorf(`fail to decode bytes from string base64-encoded: %w`, err)
			}
			return dst, nil
		}
	case strings.HasPrefix(lower, "json"):
		b, err := json.Marshal(src)
		if err != nil {
			return nil, fmt.Errorf(`fail to marshal src value to JSON: %w`, err)
		}
		dst := &spanner.NullJSON{}
		if err := dst.UnmarshalJSON(b); err != nil {
			return nil, fmt.Errorf(`fail to unmarshal value to spanner.NullJSON: %w`, err)
		}
		return dst, nil
	case strings.HasPrefix(lower, "array<"):
		inner := strings.TrimSuffix(strings.TrimPrefix(lower, "array<"), ">")
		srcRV := reflect.ValueOf(src)
		if srcRV.IsValid() {
			return nil, fmt.Errorf(`src is not valid: %v`, src)
		}
		dstRT := reflect.SliceOf(GoType(inner))
		dstRV := reflect.Zero(dstRT)
		if srcRV.Kind() == reflect.Pointer {
			if srcRV.IsNil() {
				return dstRV.Interface(), nil
			} else {
				return ConvertValue(inner, srcRV.Elem().Interface())
			}
		}
		if srcRV.Kind() != reflect.Array && srcRV.Kind() != reflect.Slice {
			return nil, fmt.Errorf(`src is not array nor slice: %v`, src)
		}
		for i := 0; i < srcRV.Len(); i++ {
			dstElm, err := ConvertValue(inner, srcRV.Index(i).Interface())
			if err != nil {
				return nil, fmt.Errorf(`fail to convert src[%d] as %s: %w`, i, inner, err)
			}
			dstRV = reflect.AppendSlice(dstRV, reflect.ValueOf(dstElm))
		}
		return dstRV.Interface(), nil
	case strings.HasPrefix(lower, "struct"):
		switch src := src.(type) {
		default:
			return nil, fmt.Errorf(`fail to convert src as spanner.NullRow: %v`, src)
		case spanner.NullRow:
			return src, nil
		}
	}
}
