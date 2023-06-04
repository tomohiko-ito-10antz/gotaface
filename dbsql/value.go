package dbsql

import (
	"database/sql"
	"fmt"
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
	if !rvDstPtr.IsValid() {
		return false
	}
	if rvDstPtr.Kind() != reflect.Pointer {
		return false
	}
	if rvDstPtr.IsNil() {
		return false
	}

	valAny := dv.Any

	nullString, errNullString := toNullString(valAny)
	nullInt64, errNullInt64 := toNullInt64(valAny)
	nullFloat64, errNullFloat64 := toNullFloat64(valAny)
	nullBool, errNullBool := toNullBool(valAny)
	nullTime, errNullTime := toNullTime(valAny)
	nullBytes, errNullBytes := toNullBytes(valAny)

	switch dstPtr := dstPtr.(type) {
	case *string, **string, *sql.NullString:
		if errNullString != nil {
			return false
		}
		err := assignToString(nullString, reflect.ValueOf(dstPtr))
		return err == nil
	case *bool, **bool, *sql.NullBool:
		if errNullBool != nil {
			return false
		}
		err := assignToBool(nullBool, reflect.ValueOf(dstPtr))
		return err == nil
	case *float32, **float32, *float64, **float64, *sql.NullFloat64:
		if errNullFloat64 != nil {
			return false
		}
		err := assignToFloat(nullFloat64, reflect.ValueOf(dstPtr))
		return err == nil
	case *int, **int, *int8, **int8, *int16, **int16, *int32, **int32, *int64, **int64,
		*uint, **uint, *uint8, **uint8, *uint16, **uint16, *uint32, **uint32, *uint64, **uint64,
		*sql.NullByte, *sql.NullInt16, *sql.NullInt32, *sql.NullInt64:
		if errNullInt64 != nil {
			return false
		}
		err := assignToInt(nullInt64, reflect.ValueOf(dstPtr))
		return err == nil
	case *time.Time, **time.Time, *sql.NullTime:
		if errNullTime != nil {
			return false
		}
		err := assignToTime(nullTime, reflect.ValueOf(dstPtr))
		return err == nil
	case *[]byte, **[]byte, *NullBytes:
		if errNullBytes != nil {
			return false
		}
		err := assignToBytes(nullBytes, reflect.ValueOf(dstPtr))
		return err == nil
	}

	return false
}

func assignToString(src sql.NullString, rvDstPtr reflect.Value) error {
	if !rvDstPtr.IsValid() {
		return fmt.Errorf(`destination must be valid`)
	}
	if rvDstPtr.Kind() != reflect.Pointer {
		return fmt.Errorf(`destination must be pointer`)
	}
	if rvDstPtr.IsNil() {
		return fmt.Errorf(`destination must not nil`)
	}

	rvDstVar := rvDstPtr.Elem()
	if !rvDstVar.CanSet() {
		return fmt.Errorf(`destination must be able to be set value`)
	}

	rtDstVar := rvDstVar.Type()

	rvSrcVal := reflect.ValueOf(src.String)
	rvSrcValPtr := reflect.ValueOf((*string)(nil))
	if src.Valid {
		rvSrcValPtr = reflect.ValueOf(&src.String)
	}
	rvSrc := reflect.ValueOf(src)

	switch {
	default:
		return fmt.Errorf(`fail to assign to destination`)
	case rvSrcVal.CanConvert(rtDstVar): // destination is pointer to string
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal.Convert(rtDstVar))
		return nil
	case rvSrcValPtr.CanConvert(rtDstVar): // destination is pointer to *string
		rvDstVar.Set(rvSrcValPtr.Convert(rtDstVar))
		return nil
	case rvSrc.CanConvert(rtDstVar): // destination is pointer to sql.NullString
		rvDstVar.Set(rvSrc.Convert(rtDstVar))
		return nil
	}
}

func assignToBool(src sql.NullBool, rvDstPtr reflect.Value) error {
	if !rvDstPtr.IsValid() {
		return fmt.Errorf(`destination must be valid`)
	}
	if rvDstPtr.Kind() != reflect.Pointer {
		return fmt.Errorf(`destination must be pointer`)
	}
	if rvDstPtr.IsNil() {
		return fmt.Errorf(`destination must not nil`)
	}

	rvDstVar := rvDstPtr.Elem()
	if !rvDstVar.CanSet() {
		return fmt.Errorf(`destination must be able to be set value`)
	}

	rtDstVar := rvDstVar.Type()

	rvSrcVal := reflect.ValueOf(src.Bool)
	rvSrcValPtr := reflect.ValueOf((*bool)(nil))
	if src.Valid {
		rvSrcValPtr = reflect.ValueOf(&src.Bool)

	}
	rvSrc := reflect.ValueOf(src)

	switch {
	default:
		return fmt.Errorf(`fail to assign to destination`)
	case rvSrcVal.CanConvert(rtDstVar): // destination is bool
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal.Convert(rtDstVar))
		return nil
	case rvSrcValPtr.CanConvert(rtDstVar): // destination is *bool
		rvDstVar.Set(rvSrcValPtr.Convert(rtDstVar))
		return nil
	case rvSrc.CanConvert(rtDstVar): // destination is sql.NullBool
		rvDstVar.Set(rvSrc.Convert(rtDstVar))
		return nil
	}
}

func assignToFloat(src sql.NullFloat64, rvDstPtr reflect.Value) error {
	if !rvDstPtr.IsValid() {
		return fmt.Errorf(`destination must be valid`)
	}
	if rvDstPtr.Kind() != reflect.Pointer {
		return fmt.Errorf(`destination must be pointer`)
	}
	if rvDstPtr.IsNil() {
		return fmt.Errorf(`destination must not nil`)
	}

	rvDstVar := rvDstPtr.Elem()
	if !rvDstVar.CanSet() {
		return fmt.Errorf(`destination must be able to be set value`)
	}

	rtDstVar := rvDstVar.Type()

	srcFloat32 := float32(src.Float64)

	rvSrcVal32 := reflect.ValueOf(srcFloat32)
	rvSrcVal64 := reflect.ValueOf(src.Float64)
	rvSrcVal32Ptr := reflect.ValueOf((*float32)(nil))
	if src.Valid {
		rvSrcVal32Ptr = reflect.ValueOf(&srcFloat32)
	}
	rvSrcVal64Ptr := reflect.ValueOf((*float64)(nil))
	if src.Valid {
		rvSrcVal64Ptr = reflect.ValueOf(&src.Float64)
	}
	rvSrc := reflect.ValueOf(src)

	switch {
	default:
		return fmt.Errorf(`fail to assign to destination`)
	case rvSrcVal32.CanConvert(rtDstVar): // destination is float32
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal32.Convert(rtDstVar))
		return nil
	case rvSrcVal64.CanConvert(rtDstVar): // destination is float64
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal64.Convert(rtDstVar))
		return nil
	case rvSrcVal32Ptr.CanConvert(rtDstVar): // destination is *float32
		rvDstVar.Set(rvSrcVal32Ptr.Convert(rtDstVar))
		return nil
	case rvSrcVal64Ptr.CanConvert(rtDstVar): // destination is *float64
		rvDstVar.Set(rvSrcVal64Ptr.Convert(rtDstVar))
		return nil
	case rvSrc.CanConvert(rtDstVar): // destination is sql.NullFloat64
		rvDstVar.Set(rvSrc.Convert(rtDstVar))
		return nil
	}
}

func assignToInt(src sql.NullInt64, rvDstPtr reflect.Value) error {
	if !rvDstPtr.IsValid() {
		return fmt.Errorf(`destination must be valid`)
	}
	if rvDstPtr.Kind() != reflect.Pointer {
		return fmt.Errorf(`destination must be pointer`)
	}
	if rvDstPtr.IsNil() {
		return fmt.Errorf(`destination must not nil`)
	}

	rvDstVar := rvDstPtr.Elem()
	if !rvDstVar.CanSet() {
		return fmt.Errorf(`destination must be able to be set value`)
	}

	rtDstVar := rvDstVar.Type()

	valInt := int(src.Int64)
	valInt8 := int8(src.Int64)
	valInt16 := int16(src.Int64)
	valInt32 := int32(src.Int64)
	valInt64 := int64(src.Int64)
	valUInt := uint(src.Int64)
	valUInt8 := uint8(src.Int64)
	valUInt16 := uint16(src.Int64)
	valUInt32 := uint32(src.Int64)
	valUInt64 := uint64(src.Int64)

	rvSrcVal := reflect.ValueOf(valInt)
	rvSrcVal8 := reflect.ValueOf(valInt8)
	rvSrcVal16 := reflect.ValueOf(valInt16)
	rvSrcVal32 := reflect.ValueOf(valInt32)
	rvSrcVal64 := reflect.ValueOf(valInt64)
	rvSrcValU := reflect.ValueOf(valUInt)
	rvSrcValU8 := reflect.ValueOf(valUInt8)
	rvSrcValU16 := reflect.ValueOf(valUInt16)
	rvSrcValU32 := reflect.ValueOf(valUInt32)
	rvSrcValU64 := reflect.ValueOf(valUInt64)

	rvSrcValPtr := reflect.ValueOf((*int)(nil))
	if src.Valid {
		rvSrcValPtr = reflect.ValueOf(&valInt)
	}
	rvSrcVal8Ptr := reflect.ValueOf((*int8)(nil))
	if src.Valid {
		rvSrcVal8Ptr = reflect.ValueOf(&valInt8)
	}
	rvSrcVal16Ptr := reflect.ValueOf((*int16)(nil))
	if src.Valid {
		rvSrcVal16Ptr = reflect.ValueOf(&valInt16)
	}
	rvSrcVal32Ptr := reflect.ValueOf((*int32)(nil))
	if src.Valid {
		rvSrcVal32Ptr = reflect.ValueOf(&valInt32)
	}
	rvSrcVal64Ptr := reflect.ValueOf((*int64)(nil))
	if src.Valid {
		rvSrcVal64Ptr = reflect.ValueOf(&valInt64)
	}
	rvSrcValUPtr := reflect.ValueOf((*uint)(nil))
	if src.Valid {
		rvSrcValUPtr = reflect.ValueOf(&valUInt)
	}
	rvSrcValU8Ptr := reflect.ValueOf((*uint8)(nil))
	if src.Valid {
		rvSrcValU8Ptr = reflect.ValueOf(&valUInt8)
	}
	rvSrcValU16Ptr := reflect.ValueOf((*uint16)(nil))
	if src.Valid {
		rvSrcValU16Ptr = reflect.ValueOf(&valUInt16)
	}
	rvSrcValU32Ptr := reflect.ValueOf((*uint32)(nil))
	if src.Valid {
		rvSrcValU32Ptr = reflect.ValueOf(&valUInt32)
	}
	rvSrcValU64Ptr := reflect.ValueOf((*uint64)(nil))
	if src.Valid {
		rvSrcValU64Ptr = reflect.ValueOf(&valUInt64)
	}

	rvSrcByte := reflect.ValueOf(sql.NullByte{Valid: src.Valid, Byte: byte(src.Int64)})
	rvSrcInt16 := reflect.ValueOf(sql.NullInt16{Valid: src.Valid, Int16: int16(src.Int64)})
	rvSrcInt32 := reflect.ValueOf(sql.NullInt32{Valid: src.Valid, Int32: int32(src.Int64)})
	rvSrcInt64 := reflect.ValueOf(src)

	switch {
	default:
		return fmt.Errorf(`fail to assign to destination`)
	case rvSrcVal.CanConvert(rtDstVar): // destination is int
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal.Convert(rtDstVar))
		return nil
	case rvSrcVal8.CanConvert(rtDstVar): // destination is int8
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal8.Convert(rtDstVar))
		return nil
	case rvSrcVal16.CanConvert(rtDstVar): // destination is int16
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal16.Convert(rtDstVar))
		return nil
	case rvSrcVal32.CanConvert(rtDstVar): // destination is int32
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal32.Convert(rtDstVar))
		return nil
	case rvSrcVal64.CanConvert(rtDstVar): // destination is int64
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal64.Convert(rtDstVar))
		return nil
	case rvSrcValU.CanConvert(rtDstVar): // destination is uint
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcValU.Convert(rtDstVar))
		return nil
	case rvSrcValU8.CanConvert(rtDstVar): // destination is uint8
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcValU8.Convert(rtDstVar))
		return nil
	case rvSrcValU16.CanConvert(rtDstVar): // destination is uint16
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcValU16.Convert(rtDstVar))
		return nil
	case rvSrcValU32.CanConvert(rtDstVar): // destination is uint32
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcValU32.Convert(rtDstVar))
		return nil
	case rvSrcValU64.CanConvert(rtDstVar): // destination is uint64
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcValU64.Convert(rtDstVar))
		return nil
	case rvSrcValPtr.CanConvert(rtDstVar): // destination is *int
		rvDstVar.Set(rvSrcValPtr.Convert(rtDstVar))
		return nil
	case rvSrcVal8Ptr.CanConvert(rtDstVar): // destination is *int8
		rvDstVar.Set(rvSrcVal8Ptr.Convert(rtDstVar))
		return nil
	case rvSrcVal16Ptr.CanConvert(rtDstVar): // destination is *int16
		rvDstVar.Set(rvSrcVal16Ptr.Convert(rtDstVar))
		return nil
	case rvSrcVal32Ptr.CanConvert(rtDstVar): // destination is *int32
		rvDstVar.Set(rvSrcVal32Ptr.Convert(rtDstVar))
		return nil
	case rvSrcVal64Ptr.CanConvert(rtDstVar): // destination is *int64
		rvDstVar.Set(rvSrcVal64Ptr.Convert(rtDstVar))
		return nil
	case rvSrcValUPtr.CanConvert(rtDstVar): // destination is *uint
		rvDstVar.Set(rvSrcValUPtr.Convert(rtDstVar))
		return nil
	case rvSrcValU8Ptr.CanConvert(rtDstVar): // destination is *uint8
		rvDstVar.Set(rvSrcValU8Ptr.Convert(rtDstVar))
		return nil
	case rvSrcValU16Ptr.CanConvert(rtDstVar): // destination is *uint16
		rvDstVar.Set(rvSrcValU16Ptr.Convert(rtDstVar))
		return nil
	case rvSrcValU32Ptr.CanConvert(rtDstVar): // destination is *uint32
		rvDstVar.Set(rvSrcValU32Ptr.Convert(rtDstVar))
		return nil
	case rvSrcValU64Ptr.CanConvert(rtDstVar): // destination is *uint64
		rvDstVar.Set(rvSrcValU64Ptr.Convert(rtDstVar))
		return nil
	case rvSrcByte.CanConvert(rtDstVar): // destination is sql.NullByte
		rvDstVar.Set(rvSrcByte.Convert(rtDstVar))
		return nil
	case rvSrcInt16.CanConvert(rtDstVar): // destination is sql.NullInt16
		rvDstVar.Set(rvSrcInt16.Convert(rtDstVar))
		return nil
	case rvSrcInt32.CanConvert(rtDstVar): // destination is sql.NullInt32
		rvDstVar.Set(rvSrcInt32.Convert(rtDstVar))
		return nil
	case rvSrcInt64.CanConvert(rtDstVar): // destination is sql.NullInt64
		rvDstVar.Set(rvSrcInt64.Convert(rtDstVar))
		return nil
	}
}

func assignToTime(src sql.NullTime, rvDstPtr reflect.Value) error {
	if !rvDstPtr.IsValid() {
		return fmt.Errorf(`destination must be valid`)
	}
	if rvDstPtr.Kind() != reflect.Pointer {
		return fmt.Errorf(`destination must be pointer`)
	}
	if rvDstPtr.IsNil() {
		return fmt.Errorf(`destination must not nil`)
	}

	rvDstVar := rvDstPtr.Elem()
	if !rvDstVar.CanSet() {
		return fmt.Errorf(`destination must be able to be set value`)
	}

	rtDstVar := rvDstVar.Type()

	rvSrcVal := reflect.ValueOf(src.Time)
	rvSrcValPtr := reflect.ValueOf((*time.Time)(nil))
	if src.Valid {
		rvSrcValPtr = reflect.ValueOf(&src.Time)
	}
	rvSrc := reflect.ValueOf(src)

	switch {
	default:
		return fmt.Errorf(`fail to assign to destination`)
	case rvSrcVal.CanConvert(rtDstVar): // destination is time.Time
		if !src.Valid {
			return fmt.Errorf(`null cannot be assign to destination`)
		}
		rvDstVar.Set(rvSrcVal.Convert(rtDstVar))
		return nil
	case rvSrcValPtr.CanConvert(rtDstVar): // destination is *time.Time
		rvDstVar.Set(rvSrcValPtr.Convert(rtDstVar))
		return nil
	case rvSrc.CanConvert(rtDstVar): // destination is sql.NullTime
		rvDstVar.Set(rvSrc.Convert(rtDstVar))
		return nil
	}
}

func assignToBytes(src NullBytes, rvDstPtr reflect.Value) error {
	if !rvDstPtr.IsValid() {
		return fmt.Errorf(`destination must be valid`)
	}
	if rvDstPtr.Kind() != reflect.Pointer {
		return fmt.Errorf(`destination must be pointer`)
	}
	if rvDstPtr.IsNil() {
		return fmt.Errorf(`destination must not nil`)
	}

	rvDstVar := rvDstPtr.Elem()
	if !rvDstVar.CanSet() {
		return fmt.Errorf(`destination must be able to be set value`)
	}

	rtDstVar := rvDstVar.Type()

	rvSrcVal := reflect.ValueOf(([]byte)(nil))
	if src.Valid {
		rvSrcVal = reflect.ValueOf(src.Bytes)
	}
	rvSrcValPtr := reflect.ValueOf((*[]byte)(nil))
	if src.Valid {
		rvSrcValPtr = reflect.ValueOf(&src.Bytes)
	}
	rvSrc := reflect.ValueOf(src)

	switch {
	default:
		return fmt.Errorf(`fail to assign to destination`)
	case rvSrcVal.CanConvert(rtDstVar): // destination is []byte
		rvDstVar.Set(rvSrcVal.Convert(rtDstVar))
		return nil
	case rvSrcValPtr.CanConvert(rtDstVar): // destination is *[]byte
		rvDstVar.Set(rvSrcValPtr.Convert(rtDstVar))
		return nil
	case rvSrc.CanConvert(rtDstVar): // destination is NullBytes
		rvDstVar.Set(rvSrc.Convert(rtDstVar))
		return nil
	}
}

func (dv *Value) ToNullString() (sql.NullString, bool) {
	val, err := toNullString(dv.Any)
	return val, err == nil
}
func (dv *Value) ToNullInt64() (sql.NullInt64, bool) {
	val, err := toNullInt64(dv.Any)
	return val, err == nil
}
func (dv *Value) ToNullBool() (sql.NullBool, bool) {
	val, err := toNullBool(dv.Any)
	return val, err == nil
}
func (dv *Value) ToNullFloat64() (sql.NullFloat64, bool) {
	val, err := toNullFloat64(dv.Any)
	return val, err == nil
}
func (dv *Value) ToNullBytes() (NullBytes, bool) {
	val, err := toNullBytes(dv.Any)
	return val, err == nil
}
func (dv *Value) ToNullTime() (sql.NullTime, bool) {
	val, err := toNullTime(dv.Any)
	return val, err == nil
}

func toNullString(value any) (sql.NullString, error) {
	switch src := value.(type) {
	case nil:
		return sql.NullString{}, nil
	case sql.NullString:
		return src, nil
	case string:
		return sql.NullString{Valid: true, String: src}, nil
	case *string:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullString{}, nil
		}
		return toNullString(rv.Elem().Interface())
	}
	var nullVal sql.NullString
	if err := nullVal.Scan(value); err != nil {
		return nullVal, fmt.Errorf(`fail to scan as sql. NullString : %w`, err)
	}
	return nullVal, nil
}
func toNullInt64(value any) (sql.NullInt64, error) {
	switch src := value.(type) {
	case nil:
		return sql.NullInt64{}, nil
	case int:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case int8:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case int16:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case int32:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case int64:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case uint:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case uint8:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case uint16:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case uint32:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case uint64:
		return sql.NullInt64{Valid: true, Int64: int64(src)}, nil
	case sql.NullByte:
		return sql.NullInt64{Valid: src.Valid, Int64: int64(src.Byte)}, nil
	case sql.NullInt16:
		return sql.NullInt64{Valid: src.Valid, Int64: int64(src.Int16)}, nil
	case sql.NullInt32:
		return sql.NullInt64{Valid: src.Valid, Int64: int64(src.Int32)}, nil
	case sql.NullInt64:
		return sql.NullInt64{Valid: src.Valid, Int64: int64(src.Int64)}, nil
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullInt64{}, nil
		}
		return toNullInt64(rv.Elem().Interface())
	default:
		var nullInt64 sql.NullInt64
		if nullInt64.Scan(value) == nil {
			return nullInt64, nil
		}
		var nullInt32 sql.NullInt32
		if nullInt32.Scan(value) == nil {
			return sql.NullInt64{Valid: nullInt32.Valid, Int64: int64(nullInt32.Int32)}, nil
		}
		var nullInt16 sql.NullInt16
		if nullInt16.Scan(value) == nil {
			return sql.NullInt64{Valid: nullInt16.Valid, Int64: int64(nullInt16.Int16)}, nil
		}
		var nullByte sql.NullByte
		if nullByte.Scan(value) == nil {
			return sql.NullInt64{Valid: nullByte.Valid, Int64: int64(nullByte.Byte)}, nil
		}
		return sql.NullInt64{}, fmt.Errorf(`fail to scan as sql. NullInt64`)
	}
}
func toNullBool(value any) (sql.NullBool, error) {
	switch src := value.(type) {
	case nil:
		return sql.NullBool{}, nil
	case bool:
		return sql.NullBool{Valid: true, Bool: src}, nil
	case sql.NullBool:
		return src, nil
	case *bool:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullBool{}, nil
		}
		return toNullBool(rv.Elem().Interface())
	}

	var nullVal sql.NullBool
	if err := nullVal.Scan(value); err != nil {
		return nullVal, fmt.Errorf(`fail to scan as sql.NullBool: %w`, err)
	}
	return nullVal, nil
}
func toNullFloat64(value any) (sql.NullFloat64, error) {
	switch src := value.(type) {
	case nil:
		return sql.NullFloat64{}, nil
	case float32:
		return sql.NullFloat64{Valid: true, Float64: float64(src)}, nil
	case float64:
		return sql.NullFloat64{Valid: true, Float64: float64(src)}, nil
	case sql.NullFloat64:
		return src, nil
	case *float32, *float64:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullFloat64{}, nil
		}
		return toNullFloat64(rv.Elem().Interface())
	}

	var nullVal sql.NullFloat64
	if err := nullVal.Scan(value); err != nil {
		return nullVal, fmt.Errorf(`fail to scan as sql.NullFloat64 : %w`, err)
	}
	return nullVal, nil
}
func toNullTime(value any) (sql.NullTime, error) {
	switch src := value.(type) {
	case nil:
		return sql.NullTime{}, nil
	case time.Time:
		return sql.NullTime{Valid: true, Time: src}, nil
	case sql.NullTime:
		return src, nil
	case *time.Time:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return sql.NullTime{}, nil
		}
		return toNullTime(rv.Elem().Interface())
	}

	var nullVal sql.NullTime
	if err := nullVal.Scan(value); err != nil {
		return nullVal, fmt.Errorf(`fail to scan as sql.NullTime : %w`, err)
	}
	return nullVal, nil
}
func toNullBytes(value any) (NullBytes, error) {
	switch src := value.(type) {
	case nil:
		return NullBytes{}, nil
	case []byte:
		if src == nil {
			return NullBytes{}, nil
		}
		return NullBytes{Valid: true, Bytes: src}, nil
	case NullBytes:
		return src, nil
	case *[]byte:
		rv := reflect.ValueOf(value)
		if rv.IsNil() {
			return NullBytes{}, nil
		}
		return toNullBytes(rv.Elem().Interface())
	default:
		return NullBytes{}, fmt.Errorf(`fail to convert to NullBytes`)
	}
}
