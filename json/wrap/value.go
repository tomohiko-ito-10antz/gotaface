package wrap

import (
	"bytes"
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
	ObjectHasElm(key string) bool
	ObjectGetElm(key string) JsonValue
	ObjectSetElm(key string, v JsonValue)
	ObjectDelElm(key string)
	ObjectLen() int
	ArrayGetElm(index int) JsonValue
	ArraySetElm(index int, v JsonValue)
	ArrayAddElm(vs ...JsonValue)
	ArrayLen() int
	ArraySlice(begin int, endExclusive int) JsonValue
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

func fromGo(a any) JsonValue {
	switch a := a.(type) {
	case nil:
		return Null()
	case json.Number:
		return Number(a)
	case string:
		return String(a)
	case bool:
		return Boolean(a)
	case []any:
		arr := Array()
		for _, a := range a {
			arr.ArrayAddElm(fromGo(a))
		}
		return arr
	case map[string]any:
		obj := Object()
		for k, a := range a {
			obj.ObjectSetElm(k, fromGo(a))
		}
		return obj
	default:
		return errors.Unexpected1[JsonValue]("unexpected value that cannot be converted to JsonValue: %#v", a)
	}
}
func (v *jsonValue) UnmarshalJSON(b []byte) error {
	decoder := json.NewDecoder(bytes.NewBuffer(b))
	decoder.UseNumber()

	var a any
	if err := decoder.Decode(&a); err != nil {
		return fmt.Errorf(`fail to unmarshal value to JsonValue: %w`, err)
	}

	v.Assign(fromGo(a))

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
			v.jsonArray[i] = other.ArrayGetElm(i)
		}
	case JsonTypeObject:
		v.jsonObject = map[string]JsonValue{}
		keys := other.ObjectKeys()
		for _, k := range keys {
			v.jsonObject[k] = other.ObjectGetElm(k)
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
			e := v.ArrayGetElm(i)
			clone.ArrayAddElm(e.Clone())
		}
		return clone
	case JsonTypeObject:
		clone := Object()
		for _, k := range v.ObjectKeys() {
			e := v.ObjectGetElm(k)
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
func (v *jsonValue) ObjectHasElm(key string) bool {
	errors.Assert(v.Type() == JsonTypeObject, "JsonValue must be JSON object")

	_, ok := v.jsonObject[key]

	return ok
}
func (v *jsonValue) ObjectGetElm(key string) JsonValue {
	errors.Assert(v.Type() == JsonTypeObject, "JsonValue must be JSON object")
	errors.Assert(v.ObjectHasElm(key), "JsonValue object must have key: %v", key)

	return v.jsonObject[key]
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
func (v *jsonValue) ArrayGetElm(index int) JsonValue {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")
	errors.Assert(0 <= index && index < v.ArrayLen(), "index must be in [0, %d)", v.ArrayLen())

	return v.jsonArray[index]
}
func (v *jsonValue) ArraySetElm(index int, val JsonValue) {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")
	errors.Assert(0 <= index && index < v.ArrayLen(), "index must be in [0, %d)", v.ArrayLen())

	v.jsonArray[index] = val
}
func (v *jsonValue) ArrayLen() int {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")

	return len(v.jsonArray)
}
func (v *jsonValue) ArrayAddElm(vals ...JsonValue) {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")

	v.jsonArray = append(v.jsonArray, vals...)
}
func (v *jsonValue) ArraySlice(begin int, endExclusive int) JsonValue {
	errors.Assert(v.Type() == JsonTypeArray, "JsonValue must be JSON array")
	errors.Assert(0 <= begin && begin <= v.ArrayLen(), "begin %v must be in [0, %d]", begin, v.ArrayLen())
	errors.Assert(0 <= endExclusive && endExclusive <= v.ArrayLen(), "endExclusive %v must be in [0, %d]", endExclusive, v.ArrayLen())
	errors.Assert(begin <= endExclusive, "begin %v and endExclusive %v must be begin <= endExclusive", begin, endExclusive)

	return Array(v.jsonArray[begin:endExclusive]...)
}
