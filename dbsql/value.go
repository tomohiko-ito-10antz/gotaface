package dbsql

import (
	"database/sql"
	"reflect"
	"time"
)

// NullBytes represents a []byte that may be null.
type NullBytes struct {
	Valid bool
	Bytes []byte
}

type Value struct {
	Any any
}

func (dv *Value) Scan(src any) error {
	dv.Any = src
	return nil
}

func (dv *Value) AssignTo(dstPtr any) bool {
	rvDstPtr := reflect.ValueOf(dstPtr)
	if !rvDstPtr.IsValid() || rvDstPtr.Kind() != reflect.Pointer || rvDstPtr.IsNil() {
		return false
	}

	nullString, canString := dv.ToNullString()
	nullInt64, canInt64 := dv.ToNullInt64()
	nullFloat64, canFloat64 := dv.ToNullFloat64()
	nullBool, canBool := dv.ToNullBool()
	nullTime, canTime := dv.ToNullTime()
	nullBytes, canBytes := dv.ToNullBytes()

	switch dstPtr := dstPtr.(type) {
	case *string:
		switch {
		case canString && nullString.Valid:
			*dstPtr = nullString.String
			return true
		}
	case **string:
		switch {
		case canString && nullString.Valid:
			*dstPtr = &nullString.String
			return true
		case canString && !nullString.Valid:
			*dstPtr = nil
			return true
		}
	case *sql.NullString:
		switch {
		case canString:
			*dstPtr = nullString
			return true
		}
	case *bool:
		switch {
		case canBool && nullBool.Valid:
			*dstPtr = nullBool.Bool
			return true
		}
	case **bool:
		switch {
		case canBool && nullBool.Valid:
			*dstPtr = &nullBool.Bool
			return true
		case canBool && !nullBool.Valid:
			*dstPtr = nil
			return true
		}
	case *sql.NullBool:
		switch {
		case canBool:
			*dstPtr = nullBool
			return true
		}
	case *float32:
		switch {
		case canFloat64 && nullFloat64.Valid:
			*dstPtr = float32(nullFloat64.Float64)
			return true
		}
	case **float32:
		switch {
		case canFloat64 && nullFloat64.Valid:
			value := float32(nullFloat64.Float64)
			*dstPtr = &value
			return true
		case canFloat64 && !nullFloat64.Valid:
			*dstPtr = nil
			return true
		}
	case *float64:
		switch {
		case canFloat64 && nullFloat64.Valid:
			*dstPtr = nullFloat64.Float64
			return true
		}
	case **float64:
		switch {
		case canFloat64 && nullFloat64.Valid:
			*dstPtr = &nullFloat64.Float64
			return true
		case canFloat64 && !nullFloat64.Valid:
			*dstPtr = nil
			return true
		}
	case *sql.NullFloat64:
		switch {
		case canFloat64:
			*dstPtr = nullFloat64
			return true
		}
	case *int:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = int(nullInt64.Int64)
			return true
		}
	case *uint:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = uint(nullInt64.Int64)
			return true
		}
	case **int:
		switch {
		case canInt64 && nullInt64.Valid:
			value := int(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case **uint:
		switch {
		case canInt64 && nullInt64.Valid:
			value := uint(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case *int8:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = int8(nullInt64.Int64)
			return true
		}
	case *uint8:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = uint8(nullInt64.Int64)
			return true
		}
	case **int8:
		switch {
		case canInt64 && nullInt64.Valid:
			value := int8(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case **uint8:
		switch {
		case canInt64 && nullInt64.Valid:
			value := uint8(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case *sql.NullByte:
		switch {
		case canInt64:
			*dstPtr = sql.NullByte{Valid: nullInt64.Valid, Byte: byte(nullInt64.Int64)}
			return true
		}
	case *int16:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = int16(nullInt64.Int64)
			return true
		}
	case *uint16:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = uint16(nullInt64.Int64)
			return true
		}
	case **int16:
		switch {
		case canInt64 && nullInt64.Valid:
			value := int16(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case **uint16:
		switch {
		case canInt64 && nullInt64.Valid:
			value := uint16(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case *sql.NullInt16:
		switch {
		case canInt64:
			*dstPtr = sql.NullInt16{Valid: nullInt64.Valid, Int16: int16(nullInt64.Int64)}
			return true
		}
	case *int32:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = int32(nullInt64.Int64)
			return true
		}
	case *uint32:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = uint32(nullInt64.Int64)
			return true
		}
	case **int32:
		switch {
		case canInt64 && nullInt64.Valid:
			value := int32(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case **uint32:
		switch {
		case canInt64 && nullInt64.Valid:
			value := uint32(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case *sql.NullInt32:
		switch {
		case canInt64:
			*dstPtr = sql.NullInt32{Valid: nullInt64.Valid, Int32: int32(nullInt64.Int64)}
			return true
		}
	case *int64:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = nullInt64.Int64
			return true
		}
	case *uint64:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = uint64(nullInt64.Int64)
			return true
		}
	case **int64:
		switch {
		case canInt64 && nullInt64.Valid:
			*dstPtr = &nullInt64.Int64
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case **uint64:
		switch {
		case canInt64 && nullInt64.Valid:
			value := uint64(nullInt64.Int64)
			*dstPtr = &value
			return true
		case canInt64 && !nullInt64.Valid:
			*dstPtr = nil
			return true
		}
	case *sql.NullInt64:
		switch {
		case canInt64:
			*dstPtr = nullInt64
			return true
		}
	case *time.Time:
		switch {
		case canTime && nullTime.Valid:
			*dstPtr = nullTime.Time
			return true
		}
	case **time.Time:
		switch {
		case canTime && nullTime.Valid:
			value := nullTime.Time
			*dstPtr = &value
			return true
		case canTime && !nullTime.Valid:
			*dstPtr = nil
			return true
		}
	case *sql.NullTime:
		switch {
		case canTime:
			*dstPtr = nullTime
			return true
		}
	case *[]byte:
		switch {
		case canBytes && nullBytes.Valid:
			*dstPtr = nullBytes.Bytes
			return true
		case canBytes && !nullBytes.Valid:
			*dstPtr = nil
			return true
		}
	case **[]byte:
		switch {
		case canBytes && nullBytes.Valid:
			value := nullBytes.Bytes
			*dstPtr = &value
			return true
		case canBytes && !nullBytes.Valid:
			*dstPtr = nil
			return true
		}
	case *NullBytes:
		switch {
		case canBytes:
			*dstPtr = nullBytes
			return true
		}
	}

	return false
}

func (dv *Value) ToNullString() (sql.NullString, bool) {
	return toNullString(dv.Any)
}
func (dv *Value) ToNullInt64() (sql.NullInt64, bool) {
	return toNullInt64(dv.Any)
}
func (dv *Value) ToNullBool() (sql.NullBool, bool) {
	return toNullBool(dv.Any)
}
func (dv *Value) ToNullFloat64() (sql.NullFloat64, bool) {
	return toNullFloat64(dv.Any)
}
func (dv *Value) ToNullBytes() (NullBytes, bool) {
	return toNullBytes(dv.Any)
}
func (dv *Value) ToNullTime() (sql.NullTime, bool) {
	return toNullTime(dv.Any)
}

func toNullString(value any) (sql.NullString, bool) {
	switch src := value.(type) {
	case nil:
		return sql.NullString{}, true
	case sql.NullString:
		return src, true
	case string:
		return sql.NullString{Valid: true, String: src}, true
	case *string:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullString{}, true
		}
		return toNullString(rv.Elem().Interface())
	default:
		var nullString sql.NullString
		if nullString.Scan(value) == nil {
			return nullString, true
		}
		return sql.NullString{}, false
	}
}
func toNullInt64(value any) (sql.NullInt64, bool) {
	switch src := value.(type) {
	case nil:
		return sql.NullInt64{}, true
	case int:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case int8:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case int16:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case int32:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case int64:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case uint:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case uint8:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case uint16:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case uint32:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case uint64:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, true
	case sql.NullByte:
		return sql.NullInt64{Valid: src.Valid, Int64: int64(src.Byte)}, true
	case sql.NullInt16:
		return sql.NullInt64{Valid: src.Valid, Int64: int64(src.Int16)}, true
	case sql.NullInt32:
		return sql.NullInt64{Valid: src.Valid, Int64: int64(src.Int32)}, true
	case sql.NullInt64:
		return sql.NullInt64{Valid: src.Valid, Int64: int64(src.Int64)}, true
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullInt64{}, true
		}
		return toNullInt64(rv.Elem().Interface())
	default:
		var nullInt64 sql.NullInt64
		if nullInt64.Scan(value) == nil {
			return nullInt64, true
		}
		var nullInt32 sql.NullInt32
		if nullInt32.Scan(value) == nil {
			return sql.NullInt64{Valid: nullInt32.Valid, Int64: int64(nullInt32.Int32)}, true
		}
		var nullInt16 sql.NullInt16
		if nullInt16.Scan(value) == nil {
			return sql.NullInt64{Valid: nullInt16.Valid, Int64: int64(nullInt16.Int16)}, true
		}
		var nullByte sql.NullByte
		if nullByte.Scan(value) == nil {
			return sql.NullInt64{Valid: nullByte.Valid, Int64: int64(nullByte.Byte)}, true
		}
		return sql.NullInt64{}, false
	}
}
func toNullBool(value any) (sql.NullBool, bool) {
	switch src := value.(type) {
	case nil:
		return sql.NullBool{}, true
	case bool:
		return sql.NullBool{Valid: true, Bool: src}, true
	case sql.NullBool:
		return src, true
	case *bool:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullBool{}, true
		}
		return toNullBool(rv.Elem().Interface())
	default:
		var nullBool sql.NullBool
		if nullBool.Scan(value) == nil {
			return nullBool, true
		}
		return sql.NullBool{}, false
	}
}
func toNullFloat64(value any) (sql.NullFloat64, bool) {
	switch src := value.(type) {
	case nil:
		return sql.NullFloat64{}, true
	case float32:
		return sql.NullFloat64{Valid: true, Float64: float64(src)}, true
	case float64:
		return sql.NullFloat64{Valid: true, Float64: float64(src)}, true
	case sql.NullFloat64:
		return src, true
	case *float32, *float64:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullFloat64{}, true
		}
		return toNullFloat64(rv.Elem().Interface())
	default:
		var nullFloat64 sql.NullFloat64
		if nullFloat64.Scan(value) == nil {
			return nullFloat64, true
		}
		return sql.NullFloat64{}, false
	}
}
func toNullTime(value any) (sql.NullTime, bool) {
	switch src := value.(type) {
	case nil:
		return sql.NullTime{}, true
	case time.Time:
		return sql.NullTime{Valid: true, Time: src}, true
	case sql.NullTime:
		return src, true
	case *time.Time:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullTime{}, true
		}
		return toNullTime(rv.Elem().Interface())
	default:
		var nullTime sql.NullTime
		if nullTime.Scan(value) == nil {
			return nullTime, true
		}
		return sql.NullTime{}, false
	}
}
func toNullBytes(value any) (NullBytes, bool) {
	switch src := value.(type) {
	case nil:
		return NullBytes{}, true
	case []byte:
		if src == nil {
			return NullBytes{}, true
		}
		return NullBytes{Valid: true, Bytes: src}, true
	case NullBytes:
		return src, true
	case *[]byte:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return NullBytes{}, true
		}
		return toNullBytes(rv.Elem().Interface())
	default:
		return NullBytes{}, false
	}
}
