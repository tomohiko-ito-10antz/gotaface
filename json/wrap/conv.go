package wrap

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/Jumpaku/gotaface/errors"
)

func ToGo(v JsonValue) any {
	switch v.Type() {
	case JsonTypeNull:
		return nil
	case JsonTypeBoolean:
		return v.BooleanGet()
	case JsonTypeNumber:
		return v.NumberGet()
	case JsonTypeString:
		return v.StringGet()
	case JsonTypeArray:
		l := v.ArrayLen()
		a := make([]any, l)
		for i := 0; i < l; i++ {
			val, _ := v.ArrayGetElm(i)
			a[i] = ToGo(val)
		}
		return a
	case JsonTypeObject:
		m := map[string]any{}
		keys := v.ObjectKeys()
		for _, key := range keys {
			val, _ := v.ObjectGetElm(key)
			m[key] = ToGo(val)
		}
		return m
	default:
		return errors.Unreachable[any]()
	}
}

func FromGo(valAny any) (JsonValue, error) {
	if v, ok := valAny.(JsonValue); ok {
		return v.Clone(), nil
	}

	switch val := valAny.(type) {
	case nil:
		return Null(), nil
	case string:
		return String(val), nil
	case json.Number:
		return Number(val), nil
	case bool:
		return Boolean(val), nil
	case map[string]any:
		if val == nil {
			return Null(), nil
		}
		o := Object()
		for k, v := range val {
			v, err := FromGo(v)
			if err != nil {
				return nil, fmt.Errorf(`fail to marshal to JsonValue: %w`, err)
			}
			o.ObjectSetElm(k, v)
		}
		return o, nil
	case []any:
		if val == nil {
			return Null(), nil
		}
		a := Array()
		for _, v := range val {
			v, err := FromGo(v)
			if err != nil {
				return nil, fmt.Errorf(`fail to marshal to JsonValue: %w`, err)
			}
			a.ArrayAddElm(v)
		}
		return a, nil
	}

	buf := bytes.NewBuffer(nil)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(valAny); err != nil {
		return nil, fmt.Errorf(`fail to marshal to JsonValue: %w`, err)
	}

	decoder := json.NewDecoder(buf)
	decoder.UseNumber()
	var val any
	if err := decoder.Decode(&val); err != nil {
		return nil, fmt.Errorf(`fail to marshal to JsonValue: %w`, err)
	}

	return FromGo(val)
}
