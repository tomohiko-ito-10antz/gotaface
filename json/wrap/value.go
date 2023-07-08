package wrap

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Jumpaku/gotaface/errors"
)

type JsonType int

func (t JsonType) String() string {
	switch t {
	case JsonTypeNull:
		return `null`
	case JsonTypeString:
		return `string`
	case JsonTypeNumber:
		return `number`
	case JsonTypeBoolean:
		return `boolean`
	case JsonTypeArray:
		return `array`
	case JsonTypeObject:
		return `object`
	default:
		panic("invalid JsonType")
	}
}

const (
	JsonTypeNull JsonType = iota
	JsonTypeString
	JsonTypeNumber
	JsonTypeBoolean
	JsonTypeArray
	JsonTypeObject
)

type JsonValue interface {
	json.Marshaler
	json.Unmarshaler
	Type() JsonType
	Assign(v JsonValue)
	Clone() JsonValue
	NumberGet() json.Number
	StringGet() string
	BooleanGet() bool
	ObjectKeys() []string
	ObjectGetElm(key string) (JsonValue, bool)
	ObjectSetElm(key string, v JsonValue)
	ObjectDelElm(key string)
	ObjectLen() int
	ArrayGetElm(index int) (JsonValue, bool)
	ArraySetElm(index int, v JsonValue) bool
	ArrayLen() int
	ArrayAddElm(vs ...JsonValue)
	ArraySlice(begin int, endExclusive int) (JsonValue, bool)
}

type jsonValue struct {
	jsonType    JsonType
	jsonNumber  json.Number
	jsonBoolean bool
	jsonString  string
	jsonObject  map[string]JsonValue
	jsonArray   []JsonValue
}

func Null() JsonValue {
	return &jsonValue{jsonType: JsonTypeNull}
}
func Boolean(b bool) JsonValue {
	return &jsonValue{jsonType: JsonTypeBoolean, jsonBoolean: b}
}
func String(s string) JsonValue {
	return &jsonValue{jsonType: JsonTypeString, jsonString: s}
}
func Number[V ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | json.Number](n V) JsonValue {
	var v json.Number
	var a any = n
	switch a := a.(type) {
	case int:
		v = json.Number(strconv.FormatInt(int64(a), 10))
	case int8:
		v = json.Number(strconv.FormatInt(int64(a), 10))
	case int16:
		v = json.Number(strconv.FormatInt(int64(a), 10))
	case int32:
		v = json.Number(strconv.FormatInt(int64(a), 10))
	case int64:
		v = json.Number(strconv.FormatInt(int64(a), 10))
	case uint:
		v = json.Number(strconv.FormatUint(uint64(a), 10))
	case uint8:
		v = json.Number(strconv.FormatUint(uint64(a), 10))
	case uint16:
		v = json.Number(strconv.FormatUint(uint64(a), 10))
	case uint32:
		v = json.Number(strconv.FormatUint(uint64(a), 10))
	case uint64:
		v = json.Number(strconv.FormatUint(uint64(a), 10))
	case float32:
		v = json.Number(strconv.FormatFloat(float64(a), 'f', 16, 64))
	case float64:
		v = json.Number(strconv.FormatFloat(float64(a), 'f', 16, 64))
	case json.Number:
		v = a
	}

	return &jsonValue{jsonType: JsonTypeNumber, jsonNumber: json.Number(v)}
}

func Object(ms ...map[string]JsonValue) JsonValue {
	o := map[string]JsonValue{}
	for _, m := range ms {
		for k, v := range m {
			errors.Assert(v != nil, "JsonValue must not be nil")
			o[k] = v
		}
	}

	return &jsonValue{jsonType: JsonTypeObject, jsonObject: o}
}
func Array(vs ...JsonValue) JsonValue {
	a := make([]JsonValue, len(vs))
	for i, v := range vs {
		errors.Assert(v != nil, "JsonValue must not be nil")
		a[i] = v
	}

	return &jsonValue{jsonType: JsonTypeArray, jsonArray: a}
}

func (v *jsonValue) MarshalJSON() ([]byte, error) {
	switch v.Type() {
	case JsonTypeNull:
		return json.Marshal(nil)
	case JsonTypeBoolean:
		return json.Marshal(v.jsonBoolean)
	case JsonTypeNumber:
		return json.Marshal(v.jsonNumber)
	case JsonTypeString:
		return json.Marshal(v.jsonString)
	case JsonTypeArray:
		return json.Marshal(v.jsonArray)
	case JsonTypeObject:
		return json.Marshal(v.jsonObject)
	default:
		return errors.Unexpected2[[]byte, error](`invalid JsonType: %v`, v.Type())
	}
}

func (v *jsonValue) UnmarshalJSON(b []byte) error {
	u, err := FromGo(json.RawMessage(b))
	if err != nil {
		return fmt.Errorf(`fail to unmarshal value to JsonValue: %w`, err)
	}
	v.Assign(u)

	return nil
}

func (v *jsonValue) Type() JsonType {
	return v.jsonType
}
func (v *jsonValue) Assign(other JsonValue) {
	v.jsonType = other.Type()
	switch other.Type() {
	case JsonTypeArray:
		l := other.ArrayLen()
		v.jsonArray = make([]JsonValue, l)
		for i := 0; i < l; i++ {
			v.jsonArray[i], _ = other.ArrayGetElm(i)
		}
	case JsonTypeObject:
		v.jsonObject = map[string]JsonValue{}
		keys := other.ObjectKeys()
		for _, k := range keys {
			v.jsonObject[k], _ = other.ObjectGetElm(k)
		}
	case JsonTypeBoolean:
		v.jsonBoolean = other.BooleanGet()
	case JsonTypeNumber:
		v.jsonNumber = other.NumberGet()
	case JsonTypeString:
		v.jsonString = other.StringGet()
	}
}

func (v *jsonValue) Clone() JsonValue {
	switch v.Type() {
	case JsonTypeArray:
		clone := Array()
		for i := 0; i < v.ArrayLen(); i++ {
			e, _ := v.ArrayGetElm(i)
			clone.ArrayAddElm(e.Clone())
		}
		return clone
	case JsonTypeObject:
		clone := Object()
		for _, k := range v.ObjectKeys() {
			e, _ := v.ObjectGetElm(k)
			clone.ObjectSetElm(k, e.Clone())
		}
		return clone
	case JsonTypeBoolean:
		return Boolean(v.BooleanGet())
	case JsonTypeNumber:
		return Number(v.NumberGet())
	case JsonTypeString:
		return String(v.StringGet())
	case JsonTypeNull:
		return Null()
	default:
		return errors.Unexpected1[JsonValue](`invalid JsonType: %v`, v.Type())
	}
}

func (v *jsonValue) NumberGet() json.Number {
	errors.Assert(v.Type() == JsonTypeNumber, "JsonValue must be JSON number")

	return v.jsonNumber
}
func (v *jsonValue) StringGet() string {
	errors.Assert(v.Type() == JsonTypeString, "JsonValue must be JSON string")

	return v.jsonString
}
func (v *jsonValue) BooleanGet() bool {
	errors.Assert(v.Type() == JsonTypeBoolean, "JsonValue must be JSON boolean")

	return v.jsonBoolean
}
func (v *jsonValue) ObjectKeys() []string {
	errors.Assert(v.Type() == JsonTypeObject, "JsonValue must be JSON object")

	keys := []string{}
	for key := range v.jsonObject {
		keys = append(keys, key)
	}

	return keys
}
func (v *jsonValue) ObjectGetElm(key string) (JsonValue, bool) {
	errors.Assert(v.Type() == JsonTypeObject, "JsonValue must be JSON object")

	val, ok := v.jsonObject[key]

	return val, ok
}
func (v *jsonValue) ObjectSetElm(key string, val JsonValue) {
	errors.Assert(v.Type() == JsonTypeObject, "JsonValue must be JSON object")
	errors.Assert(val != nil, "JsonValue must be not nil")

	v.jsonObject[key] = val
}
func (v *jsonValue) ObjectDelElm(key string) {
	errors.Assert(v.Type() == JsonTypeObject, "JsonValue must be JSON object")

	delete(v.jsonObject, key)
}
func (v *jsonValue) ObjectLen() int {
	errors.Assert(v.Type() == JsonTypeObject, "JsonValue must be JSON object")

	return len(v.jsonObject)
}
func (v *jsonValue) ArrayGetElm(index int) (JsonValue, bool) {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")

	if index < 0 || index >= v.ArrayLen() {
		return nil, false
	}

	return v.jsonArray[index], true
}
func (v *jsonValue) ArraySetElm(index int, val JsonValue) bool {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")

	if index < 0 || index >= v.ArrayLen() {
		return false
	}

	v.jsonArray[index] = val

	return true
}
func (v *jsonValue) ArrayLen() int {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")

	return len(v.jsonArray)
}
func (v *jsonValue) ArrayAddElm(vals ...JsonValue) {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")

	v.jsonArray = append(v.jsonArray, vals...)
}
func (v *jsonValue) ArraySlice(begin int, endExclusive int) (JsonValue, bool) {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")

	if begin < 0 || begin >= v.ArrayLen() {
		return nil, false
	}

	if endExclusive < begin || endExclusive > v.ArrayLen() {
		return nil, false
	}

	return Array(v.jsonArray[begin:endExclusive]...), true
}
