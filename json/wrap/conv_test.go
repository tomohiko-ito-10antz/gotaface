package wrap_test

import (
	"encoding/json"
	"testing"

	"github.com/Jumpaku/gotaface/json/wrap"
	"github.com/Jumpaku/gotaface/test/assert"
)

func TestToGo_Nil(t *testing.T) {
	v := wrap.ToGo(wrap.Null())
	assert.Equal(t, v, nil)
}
func TestToGo_Number(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		v := wrap.ToGo(wrap.Number(-123.45))
		a, err := v.(json.Number).Float64()
		assert.Equal(t, err, nil)
		assert.CloseTo(t, a, -123.45, 1e-9)
	})
	t.Run("integer", func(t *testing.T) {
		v := wrap.ToGo(wrap.Number(123))
		a, err := v.(json.Number).Int64()
		assert.Equal(t, err, nil)
		assert.Equal(t, a, 123)
	})
}
func TestToGo_String(t *testing.T) {
	v := wrap.ToGo(wrap.String("abc"))
	assert.Equal(t, v.(string), "abc")
}
func TestToGo_Boolean(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		v := wrap.ToGo(wrap.Boolean(true))
		assert.Equal(t, v.(bool), true)
	})
	t.Run("false", func(t *testing.T) {
		v := wrap.ToGo(wrap.Boolean(false))
		assert.Equal(t, v.(bool), false)
	})
}
func TestToGo_Object(t *testing.T) {
	v := wrap.ToGo(wrap.Object(map[string]wrap.JsonValue{
		"a": wrap.Null(),
		"b": wrap.Number(123),
		"c": wrap.String("abc"),
		"d": wrap.Boolean(true),
		"e": wrap.Object(),
		"f": wrap.Array(),
	})).(map[string]any)
	assert.Equal(t, v["a"], nil)
	i, _ := v["b"].(json.Number).Int64()
	assert.Equal(t, i, 123)
	assert.Equal(t, v["c"].(string), "abc")
	assert.Equal(t, v["d"].(bool), true)
	assert.Equal(t, len(v["e"].(map[string]any)), 0)
	assert.Equal(t, len(v["f"].([]any)), 0)
}
func TestToGo_Array(t *testing.T) {
	t.Run("[]any", func(t *testing.T) {
		v := wrap.ToGo(wrap.Array(
			wrap.Null(),
			wrap.Number(123),
			wrap.String("abc"),
			wrap.Boolean(true),
			wrap.Object(),
			wrap.Array(),
		)).([]any)
		assert.Equal(t, v[0], nil)
		i, _ := v[1].(json.Number).Int64()
		assert.Equal(t, i, 123)
		assert.Equal(t, v[2].(string), "abc")
		assert.Equal(t, v[3].(bool), true)
		assert.Equal(t, len(v[4].(map[string]any)), 0)
		assert.Equal(t, len(v[5].([]any)), 0)
	})
}

func TestFromGo_Nil(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		v, err := wrap.FromGo(nil)
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})
	t.Run("(*int)(nil)", func(t *testing.T) {
		v, err := wrap.FromGo((*int)(nil))
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})
	t.Run("(*string)(nil)", func(t *testing.T) {
		v, err := wrap.FromGo((*string)(nil))
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})
	t.Run("(*bool)(nil)", func(t *testing.T) {
		v, err := wrap.FromGo((*bool)(nil))
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})
	t.Run("(*struct{})(nil)", func(t *testing.T) {
		v, err := wrap.FromGo((*struct{})(nil))
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})
	t.Run("(map[string]any)(nil)", func(t *testing.T) {
		v, err := wrap.FromGo((map[string]any)(nil))
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})
	t.Run("([]any)(nil)", func(t *testing.T) {
		v, err := wrap.FromGo(([]any)(nil))
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})
}

func TestFromGo_Number(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		t.Run("json.Number", func(t *testing.T) {
			v, err := wrap.FromGo(json.Number("-123.45"))
			a, _ := v.NumberGet().Float64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.CloseTo(t, a, -123.45, 1e-9)
		})
		t.Run("float32", func(t *testing.T) {
			v, err := wrap.FromGo(float32(-123.45))
			a, _ := v.NumberGet().Float64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.CloseTo(t, a, -123.45, 1e-9)
		})
		t.Run("float64", func(t *testing.T) {
			v, err := wrap.FromGo(float64(-123.45))
			a, _ := v.NumberGet().Float64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.CloseTo(t, a, -123.45, 1e-9)
		})
	})
	t.Run("integer", func(t *testing.T) {
		t.Run("json.Number", func(t *testing.T) {
			v, err := wrap.FromGo(json.Number("123"))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("int", func(t *testing.T) {
			v, err := wrap.FromGo(int(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("int8", func(t *testing.T) {
			v, err := wrap.FromGo(int8(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("int16", func(t *testing.T) {
			v, err := wrap.FromGo(int16(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("int32", func(t *testing.T) {
			v, err := wrap.FromGo(int32(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("int64", func(t *testing.T) {
			v, err := wrap.FromGo(int64(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("uint", func(t *testing.T) {
			v, err := wrap.FromGo(uint(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("uint8", func(t *testing.T) {
			v, err := wrap.FromGo(uint8(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("uint16", func(t *testing.T) {
			v, err := wrap.FromGo(uint16(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("uint32", func(t *testing.T) {
			v, err := wrap.FromGo(uint32(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
		t.Run("uint64", func(t *testing.T) {
			v, err := wrap.FromGo(uint64(123))
			a, _ := v.NumberGet().Int64()
			assert.Equal(t, err, nil)
			assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
			assert.Equal(t, a, 123)
		})
	})
}

func TestFromGo_String(t *testing.T) {
	v, err := wrap.FromGo("abc")
	assert.Equal(t, err, nil)
	assert.Equal(t, v.Type(), wrap.JsonTypeString)
	assert.Equal(t, v.StringGet(), "abc")
}

func TestFromGo_Boolean(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		v, err := wrap.FromGo(true)
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeBoolean)
		assert.Equal(t, v.BooleanGet(), true)
	})
	t.Run("false", func(t *testing.T) {
		v, err := wrap.FromGo(false)
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeBoolean)
		assert.Equal(t, v.BooleanGet(), false)
	})
}

func TestFromGo_Object(t *testing.T) {
	t.Run("map[string]any", func(t *testing.T) {
		v, err := wrap.FromGo(map[string]any{
			"a": nil,
			"b": 123,
			"c": "abc",
			"d": true,
			"e": map[string]any{},
			"f": []any{},
		})
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeObject)

		a, ok := v.ObjectGetElm("a")
		assert.Equal(t, ok, true)
		assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		b, ok := v.ObjectGetElm("b")
		assert.Equal(t, ok, true)
		i, _ := b.NumberGet().Int64()
		assert.Equal(t, i, 123)
		c, ok := v.ObjectGetElm("c")
		assert.Equal(t, ok, true)
		assert.Equal[any](t, c.StringGet(), "abc")
		d, ok := v.ObjectGetElm("d")
		assert.Equal(t, ok, true)
		assert.Equal[any](t, d.BooleanGet(), true)
		e, ok := v.ObjectGetElm("e")
		assert.Equal(t, ok, true)
		assert.Equal[any](t, e.Type(), wrap.JsonTypeObject)
		f, ok := v.ObjectGetElm("f")
		assert.Equal(t, ok, true)
		assert.Equal[any](t, f.Type(), wrap.JsonTypeArray)
	})
	t.Run("struct", func(t *testing.T) {
		v, err := wrap.FromGo(struct {
			A any
			B any
			C any
			D any
			E any
			F any
		}{
			A: nil,
			B: 123,
			C: "abc",
			D: true,
			E: map[string]any{},
			F: []any{},
		})
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeObject)

		a, ok := v.ObjectGetElm("A")
		assert.Equal(t, ok, true)
		assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		b, ok := v.ObjectGetElm("B")
		assert.Equal(t, ok, true)
		i, _ := b.NumberGet().Int64()
		assert.Equal(t, i, 123)
		c, ok := v.ObjectGetElm("C")
		assert.Equal(t, ok, true)
		assert.Equal[any](t, c.StringGet(), "abc")
		d, ok := v.ObjectGetElm("D")
		assert.Equal(t, ok, true)
		assert.Equal[any](t, d.BooleanGet(), true)
		e, ok := v.ObjectGetElm("E")
		assert.Equal(t, ok, true)
		assert.Equal[any](t, e.Type(), wrap.JsonTypeObject)
		f, ok := v.ObjectGetElm("F")
		assert.Equal(t, ok, true)
		assert.Equal[any](t, f.Type(), wrap.JsonTypeArray)
	})
}

func TestFromGo_Array(t *testing.T) {
	t.Run("[]any", func(t *testing.T) {
		v, err := wrap.FromGo([]any{
			nil,
			123,
			"abc",
			true,
			map[string]any{},
			[]any{},
		})
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeArray)

		a, ok := v.ArrayGetElm(0)
		assert.Equal(t, ok, true)
		assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		b, ok := v.ArrayGetElm(1)
		assert.Equal(t, ok, true)
		i, _ := b.NumberGet().Int64()
		assert.Equal(t, i, 123)
		c, ok := v.ArrayGetElm(2)
		assert.Equal(t, ok, true)
		assert.Equal[any](t, c.StringGet(), "abc")
		d, ok := v.ArrayGetElm(3)
		assert.Equal(t, ok, true)
		assert.Equal[any](t, d.BooleanGet(), true)
		e, ok := v.ArrayGetElm(4)
		assert.Equal(t, ok, true)
		assert.Equal[any](t, e.Type(), wrap.JsonTypeObject)
		f, ok := v.ArrayGetElm(5)
		assert.Equal(t, ok, true)
		assert.Equal[any](t, f.Type(), wrap.JsonTypeArray)
	})
	t.Run("[6]any", func(t *testing.T) {
		v, err := wrap.FromGo([6]any{
			nil,
			123,
			"abc",
			true,
			map[string]any{},
			[]any{},
		})
		assert.Equal(t, err, nil)
		assert.Equal(t, v.Type(), wrap.JsonTypeArray)

		a, ok := v.ArrayGetElm(0)
		assert.Equal(t, ok, true)
		assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		b, ok := v.ArrayGetElm(1)
		assert.Equal(t, ok, true)
		i, _ := b.NumberGet().Int64()
		assert.Equal(t, i, 123)
		c, ok := v.ArrayGetElm(2)
		assert.Equal(t, ok, true)
		assert.Equal[any](t, c.StringGet(), "abc")
		d, ok := v.ArrayGetElm(3)
		assert.Equal(t, ok, true)
		assert.Equal[any](t, d.BooleanGet(), true)
		e, ok := v.ArrayGetElm(4)
		assert.Equal(t, ok, true)
		assert.Equal[any](t, e.Type(), wrap.JsonTypeObject)
		f, ok := v.ArrayGetElm(5)
		assert.Equal(t, ok, true)
		assert.Equal[any](t, f.Type(), wrap.JsonTypeArray)
	})
}
