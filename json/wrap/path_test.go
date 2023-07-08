package wrap_test

import (
	"fmt"
	"testing"

	"github.com/Jumpaku/gotaface/json/wrap"
	"github.com/Jumpaku/gotaface/test/assert"
)

func TestKey_String(t *testing.T) {
	v := wrap.Key("abc").String()
	assert.Equal(t, v, "abc")
}

func TestKey_Integer(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		v := wrap.Key("123").Integer()
		assert.Equal(t, v, 123)
	})
}

func TestPath_Equals(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		p := wrap.Path([]wrap.Key{"abc", "123"})
		assert.Equal(t, p.Equals(wrap.Path([]wrap.Key{"abc", "123"})), true)
	})
	t.Run("not equal", func(t *testing.T) {
		p := wrap.Path([]wrap.Key{"abc", "123"})
		assert.Equal(t, p.Equals(wrap.Path([]wrap.Key{"abc", "123", "xyz"})), false)
	})
}

func TestPath_Get(t *testing.T) {
	p := wrap.Path([]wrap.Key{"abc", "123"})
	k0 := p.Get(0)
	assert.Equal(t, k0, wrap.Key("abc"))
	k1 := p.Get(1)
	assert.Equal(t, k1, wrap.Key("123"))
}

func TestPath_Len(t *testing.T) {
	p := wrap.Path([]wrap.Key{"abc", "123"})
	assert.Equal(t, p.Len(), 2)
}

func TestPath_Append(t *testing.T) {
	p := wrap.Path([]wrap.Key{"abc", "123"}).Append("xyz")
	assert.Equal(t, p.Len(), 3)
	k1 := p.Get(2)
	assert.Equal(t, k1, wrap.Key("xyz"))
}

func TestWalk(t *testing.T) {
	t.Run(`error`, func(t *testing.T) {
		v := wrap.Null()
		err := wrap.Walk(v, func(path wrap.Path, val wrap.JsonValue) error {
			return fmt.Errorf("")
		})
		assert.IsNotNil(t, err)
	})
	t.Run(`null`, func(t *testing.T) {
		v := wrap.Null()
		p := []wrap.Path{}
		_ = wrap.Walk(v, func(path wrap.Path, val wrap.JsonValue) error {
			p = append(p, path)
			return nil
		})
		assert.Equal(t, len(p), 1)
	})
	t.Run(`object`, func(t *testing.T) {
		v := wrap.Object(map[string]wrap.JsonValue{
			"a": wrap.Null(),
			"b": wrap.Object(map[string]wrap.JsonValue{
				"x": wrap.Null(),
				"y": wrap.Object(map[string]wrap.JsonValue{
					"w": wrap.Null(),
				}),
				"z": wrap.Array(
					wrap.Null(),
				),
			}),
			"c": wrap.Array(
				wrap.Null(),
				wrap.Object(map[string]wrap.JsonValue{
					"w": wrap.Null(),
				}),
				wrap.Array(
					wrap.Null(),
				),
			),
		})
		p := []wrap.Path{}
		_ = wrap.Walk(v, func(path wrap.Path, val wrap.JsonValue) error {
			p = append(p, path)
			return nil
		})
		assert.Equal(t, len(p), 14)
	})
	t.Run(`array`, func(t *testing.T) {
		v := wrap.Array(
			wrap.Null(),
			wrap.Object(map[string]wrap.JsonValue{
				"x": wrap.Null(),
				"y": wrap.Object(map[string]wrap.JsonValue{
					"w": wrap.Null(),
				}),
				"z": wrap.Array(
					wrap.Null(),
				),
			}),
			wrap.Array(
				wrap.Null(),
				wrap.Object(map[string]wrap.JsonValue{
					"w": wrap.Null(),
				}),
				wrap.Array(
					wrap.Null(),
				),
			),
		)
		p := []wrap.Path{}
		_ = wrap.Walk(v, func(path wrap.Path, val wrap.JsonValue) error {
			p = append(p, path)
			return nil
		})
		assert.Equal(t, len(p), 14)
	})
}
func TestFind(t *testing.T) {
	t.Run(`not found`, func(t *testing.T) {
		t.Run(`null`, func(t *testing.T) {
			v := wrap.Null()
			_, ok := wrap.Find(v, wrap.Path{"xxx"})
			assert.Equal(t, ok, false)
		})
		t.Run(`object`, func(t *testing.T) {
			v := wrap.Object(map[string]wrap.JsonValue{
				"a": wrap.Null(),
				"b": wrap.Object(map[string]wrap.JsonValue{
					"x": wrap.Null(),
					"y": wrap.Object(map[string]wrap.JsonValue{
						"w": wrap.Null(),
					}),
					"z": wrap.Array(
						wrap.Null(),
					),
				}),
				"c": wrap.Array(
					wrap.Null(),
					wrap.Object(map[string]wrap.JsonValue{
						"w": wrap.Null(),
					}),
					wrap.Array(
						wrap.Null(),
					),
				),
			})
			_, ok := wrap.Find(v, wrap.Path{"xxx"})
			assert.Equal(t, ok, false)
		})
		t.Run(`array`, func(t *testing.T) {
			v := wrap.Array(
				wrap.Null(),
				wrap.Object(map[string]wrap.JsonValue{
					"x": wrap.Null(),
					"y": wrap.Object(map[string]wrap.JsonValue{
						"w": wrap.Null(),
					}),
					"z": wrap.Array(
						wrap.Null(),
					),
				}),
				wrap.Array(
					wrap.Null(),
					wrap.Object(map[string]wrap.JsonValue{
						"w": wrap.Null(),
					}),
					wrap.Array(
						wrap.Null(),
					),
				),
			)
			_, ok := wrap.Find(v, wrap.Path{"xxx"})
			assert.Equal(t, ok, false)
		})
	})
	t.Run(`null`, func(t *testing.T) {
		v := wrap.Null()
		a, ok := wrap.Find(v, wrap.Path{})
		assert.Equal(t, ok, true)
		assert.Equal(t, a.Type(), wrap.JsonTypeNull)
	})
	t.Run(`object`, func(t *testing.T) {
		v := wrap.Object(map[string]wrap.JsonValue{
			"a": wrap.Null(),
			"b": wrap.Object(map[string]wrap.JsonValue{
				"x": wrap.Null(),
				"y": wrap.Object(map[string]wrap.JsonValue{
					"w": wrap.Null(),
				}),
				"z": wrap.Array(
					wrap.Null(),
				),
			}),
			"c": wrap.Array(
				wrap.Null(),
				wrap.Object(map[string]wrap.JsonValue{
					"w": wrap.Null(),
				}),
				wrap.Array(
					wrap.Null(),
				),
			),
		})
		t.Run(".", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeObject)
		})
		t.Run(".a", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"a"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".b", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"b"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeObject)
		})
		t.Run(".b.x", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"b", "x"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".b.y", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"b", "y"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeObject)
		})
		t.Run(".b.y.w", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"b", "y", "w"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".b.z", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"b", "z"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeArray)
		})
		t.Run(".b.z.0", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"b", "z", "0"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".c", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"c"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeArray)
		})
		t.Run(".c.0", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"c", "0"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".c.1", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"c", "1"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeObject)
		})
		t.Run(".c.1.w", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"c", "1", "w"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".c.2", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"c", "2"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeArray)
		})
		t.Run(".c.2.0", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"c", "2", "0"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
	})
	t.Run(`array`, func(t *testing.T) {
		v := wrap.Array(
			wrap.Null(),
			wrap.Object(map[string]wrap.JsonValue{
				"x": wrap.Null(),
				"y": wrap.Object(map[string]wrap.JsonValue{
					"w": wrap.Null(),
				}),
				"z": wrap.Array(
					wrap.Null(),
				),
			}),
			wrap.Array(
				wrap.Null(),
				wrap.Object(map[string]wrap.JsonValue{
					"w": wrap.Null(),
				}),
				wrap.Array(
					wrap.Null(),
				),
			),
		)
		t.Run(".", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeArray)
		})
		t.Run(".0", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"0"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".1", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"1"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeObject)
		})
		t.Run(".1.x", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"1", "x"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".1.y", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"1", "y"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeObject)
		})
		t.Run(".1.y.w", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"1", "y", "w"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".1.z", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"1", "z"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeArray)
		})
		t.Run(".1.z.0", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"1", "z", "0"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".2", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"2"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeArray)
		})
		t.Run(".2.0", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"2", "0"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".2.1", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"2", "1"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeObject)
		})
		t.Run(".2.1.w", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"2", "1", "w"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
		t.Run(".2.2", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"2", "2"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeArray)
		})
		t.Run(".2.2.0", func(t *testing.T) {
			a, ok := wrap.Find(v, wrap.Path{"2", "2", "0"})
			assert.Equal(t, ok, true)
			assert.Equal(t, a.Type(), wrap.JsonTypeNull)
		})
	})
}
