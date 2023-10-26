package spanner

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
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

func mustInt64(v any) int64 {
	switch v := v.(type) {
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
	default:
		panic(`cannot convert to int64`)
	}
}
func mustUint64(v any) uint64 {
	switch v := v.(type) {
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return uint64(v)
	default:
		panic(`cannot convert to uint64`)
	}
}
func mustFloat64(v any) float64 {
	switch v := v.(type) {
	case float32:
		return float64(v)
	case float64:
		return float64(v)
	default:
		panic(`cannot convert to float64`)
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
		return nil, fmt.Errorf(`unsupported column type: %v`, columnType)
	case strings.HasPrefix(lower, "int64"):
		switch src := src.(type) {
		case json.Number:
			dst, err := src.Int64()
			if err != nil {
				return nil, fmt.Errorf(`fail to convert %v as NullInt64: %w`, spew.Sdump(src), err)
			}
			return spanner.NullInt64{Valid: true, Int64: dst}, nil
		case int, int8, int16, int32:
			return ToDBValue(columnType, mustInt64(src))
		case uint, uint8, uint16, uint32, uint64:
			return ToDBValue(columnType, int64(mustUint64(src)))
		default:
			dst := &spanner.NullInt64{}
			if err := dst.Scan(src); err != nil {
				return nil, fmt.Errorf(`fail to scan %v as NullInt64: %w`, spew.Sdump(src), err)
			}
			return *dst, nil
		}
	case strings.HasPrefix(lower, "string"):
		dst := &spanner.NullString{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan %v as NullString: %w`, spew.Sdump(src), err)
		}
		return *dst, nil
	case strings.HasPrefix(lower, "bool"):
		dst := &spanner.NullBool{}
		if err := dst.Scan(src); err != nil {
			return nil, fmt.Errorf(`fail to scan %v as NullBool: %w`, spew.Sdump(src), err)
		}
		return *dst, nil
	case strings.HasPrefix(lower, "float64"):
		switch src := src.(type) {
		case json.Number:
			dst, err := src.Float64()
			if err != nil {
				return nil, fmt.Errorf(`fail to convert %v as NullFloat64: %w`, spew.Sdump(src), err)
			}
			return spanner.NullFloat64{Valid: true, Float64: dst}, nil
		case float32:
			return ToDBValue(columnType, float64(src))
		default:
			dst := &spanner.NullFloat64{}
			if err := dst.Scan(src); err != nil {
				return nil, fmt.Errorf(`fail to scan %v as NullFloat64: %w`, spew.Sdump(src), err)
			}
			return *dst, nil
		}
	case strings.HasPrefix(lower, "timestamp"):
		switch src := src.(type) {
		case string:
			dst, err := time.Parse(time.RFC3339, src)
			if err != nil {
				return nil, fmt.Errorf(`fail to parse %v as NullTime: %w`, spew.Sdump(src), err)
			}
			return ToDBValue(columnType, dst)
		default:
			dst := &spanner.NullTime{}
			if err := dst.Scan(src); err != nil {
				return nil, fmt.Errorf(`fail to scan %v as NullTime: %w`, spew.Sdump(src), err)
			}
			return *dst, nil
		}

	case strings.HasPrefix(lower, "date"):
		switch src := src.(type) {
		case string:
			dst, err := civil.ParseDate(src)
			if err != nil {
				return nil, fmt.Errorf(`fail to parse %v as NullDate: %w`, spew.Sdump(src), err)
			}
			return ToDBValue(columnType, dst)
		default:
			dst := &spanner.NullDate{}
			if err := dst.Scan(src); err != nil {
				return nil, fmt.Errorf(`fail to scan %v as NullDate: %w`, spew.Sdump(src), err)
			}
			return *dst, nil
		}
	case strings.HasPrefix(lower, "numeric"):
		switch src := src.(type) {
		case int, int8, int16, int32, int64:
			v := (&big.Rat{}).SetInt64(mustInt64(src))
			return spanner.NullNumeric{Valid: true, Numeric: *v}, nil
		case uint, uint8, uint16, uint32, uint64:
			v := (&big.Rat{}).SetUint64(mustUint64(src))
			return spanner.NullNumeric{Valid: true, Numeric: *v}, nil
		case float32, float64:
			v := (&big.Rat{}).SetFloat64(mustFloat64(src))
			return spanner.NullNumeric{Valid: true, Numeric: *v}, nil
		case json.Number:
			v, ok := (&big.Rat{}).SetString(string(src))
			if !ok {
				return nil, fmt.Errorf(`fail to scan %v as NullNumeric`, spew.Sdump(src))
			}
			return spanner.NullNumeric{Valid: true, Numeric: *v}, nil
		default:
			dst := &spanner.NullNumeric{}
			if err := dst.Scan(src); err != nil {
				return nil, fmt.Errorf(`fail to scan %v as NullNumeric: %w`, spew.Sdump(src), err)
			}
			return *dst, nil
		}
	case strings.HasPrefix(lower, "bytes"):
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
	case strings.HasPrefix(lower, "json"):
		b, err := json.Marshal(src)
		if err != nil {
			return nil, fmt.Errorf(`fail to marshal src %v to JSON: %w`, spew.Sdump(src), err)
		}
		dst := &spanner.NullJSON{}
		if err := dst.UnmarshalJSON(b); err != nil {
			return nil, fmt.Errorf(`fail to unmarshal to spanner.NullJSON: %w`, err)
		}
		return *dst, nil
	case strings.HasPrefix(lower, "array<"):
		inner := strings.TrimSuffix(strings.TrimPrefix(lower, "array<"), ">")
		srcRV := reflect.ValueOf(src)
		if !srcRV.IsValid() {
			return nil, fmt.Errorf(`src is not valid: %v`, spew.Sdump(src))
		}

		if srcRV.Kind() != reflect.Array && srcRV.Kind() != reflect.Slice {
			return nil, fmt.Errorf(`src is not array nor slice: %v`, spew.Sdump(src))
		}

		if srcRV.IsNil() {
			return reflect.Zero(reflect.SliceOf(GoType(inner))).Interface(), nil
		}

		dstRV := reflect.MakeSlice(reflect.SliceOf(GoType(inner)), srcRV.Len(), srcRV.Cap())
		for i := 0; i < srcRV.Len(); i++ {
			dstElm, err := ToDBValue(inner, srcRV.Index(i).Interface())
			if err != nil {
				return nil, fmt.Errorf(`fail to convert src[%d] in %v as %s: %w`, i, spew.Sdump(src), inner, err)
			}
			dstRV.Index(i).Set(reflect.ValueOf(dstElm))
		}
		return dstRV.Interface(), nil
	case strings.HasPrefix(lower, "struct"):
		switch src := src.(type) {
		default:
			return nil, fmt.Errorf(`fail to convert to spanner.NullRow: %v`, spew.Sdump(src))
		case spanner.Row:
			return spanner.NullRow{Valid: true, Row: src}, nil
		case spanner.NullRow:
			return src, nil
		}
	}
}
