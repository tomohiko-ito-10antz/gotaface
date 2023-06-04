package wrap_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/Jumpaku/gotaface/json/wrap"
	"github.com/Jumpaku/gotaface/test/assert"
	"golang.org/x/exp/slices"
)

func closeTo(t *testing.T, got any, want any, tolerance float64) bool {
	t.Helper()

	var g float64
	var w float64
	switch got := got.(type) {
	case float32:
		g = float64(got)
	case float64:
		g = float64(got)
	default:
		t.Fatalf(`got value is not float`)
	}
	switch want := want.(type) {
	case float32:
		w = float64(want)
	case float64:
		w = float64(want)
	default:
		t.Fatalf(`want value is not float`)
	}

	return math.Abs(g-w) <= tolerance
}

func TestType(t *testing.T) {
	type testCase struct {
		sut  wrap.JsonValue
		want wrap.JsonType
	}
	testCases := []testCase{
		{
			sut:  wrap.Null(),
			want: wrap.JsonTypeNull,
		},
		{
			sut:  wrap.Number(123),
			want: wrap.JsonTypeNumber,
		},
		{
			sut:  wrap.String("abc"),
			want: wrap.JsonTypeString,
		},
		{
			sut:  wrap.Boolean(true),
			want: wrap.JsonTypeBoolean,
		},
		{
			sut:  wrap.Array(),
			want: wrap.JsonTypeArray,
		},
		{
			sut:  wrap.Object(),
			want: wrap.JsonTypeObject,
		},
	}

	for i, testCase := range testCases {
		got := testCase.sut.Type()
		if got != testCase.want {
			t.Errorf("case=%d: got != want\n  got  = %v\n  want = %v", i, got, testCase.want)
		}
	}
}

func TestNumberGet(t *testing.T) {
	type testCase struct {
		sut  wrap.JsonValue
		want any
	}
	testCases := []testCase{
		{
			sut:  wrap.Number(json.Number("-123.45")),
			want: float64(-123.45),
		},
		{
			sut:  wrap.Number(float32(-123.5)),
			want: float64(-123.5),
		},
		{
			sut:  wrap.Number(float64(-123.45)),
			want: float64(-123.45),
		},
		{
			sut:  wrap.Number(json.Number("123")),
			want: int64(123),
		},
		{
			sut:  wrap.Number(int(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(int8(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(int16(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(int32(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(int64(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(uint(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(uint8(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(uint16(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(uint32(123)),
			want: int64(123),
		},
		{
			sut:  wrap.Number(uint64(123)),
			want: int64(123),
		},
	}
	for i, testCase := range testCases {
		got := testCase.sut.NumberGet()
		switch want := testCase.want.(type) {
		case float64:
			got, err := got.Float64()
			if err != nil {
				t.Errorf("case=%d: err = %#v", i, err)
			}
			if !closeTo(t, got, want, 1e-10) {
				t.Errorf("case=%d: got is not close to want\n  got  = %#v\n  want = %#v", i, got, want)
			}
		case int64:
			got, err := got.Int64()
			if err != nil {
				t.Errorf("case=%d: err = %#v", i, err)
			}
			if got != want {
				t.Errorf("case=%d: got != want\n  got  = %#v\n  want = %#v", i, got, want)
			}
		default:
			t.Fatal()
		}
	}
}

func TestStringGet(t *testing.T) {
	want := "abc"
	sut := wrap.String("abc")
	got := sut.StringGet()
	assert.Equal(t, got, want)
}

func TestBooleanGet(t *testing.T) {
	t.Run(`true`, func(t *testing.T) {
		want := true
		sut := wrap.Boolean(true)
		got := sut.BooleanGet()
		assert.Equal(t, got, want)
	})

	t.Run(`false`, func(t *testing.T) {
		want := false
		sut := wrap.Boolean(false)
		got := sut.BooleanGet()
		assert.Equal(t, got, want)
	})
}

func checkSliceContains[T comparable](t *testing.T, a []T, val T) {
	t.Helper()
	if !slices.Contains(a, val) {
		t.Errorf(`"slice does not have %#v", val`, val)
	}
}
func TestObjectKeys(t *testing.T) {
	o := wrap.Object(map[string]wrap.JsonValue{
		"a": wrap.Null(),
		"b": wrap.Number(123),
		"c": wrap.String("abc"),
		"d": wrap.Boolean(true),
		"e": wrap.Object(),
		"f": wrap.Array(),
	})
	aKeys := o.ObjectKeys()

	assert.Equal(t, len(aKeys), 6)
	checkSliceContains(t, aKeys, "a")
	checkSliceContains(t, aKeys, "b")
	checkSliceContains(t, aKeys, "c")
	checkSliceContains(t, aKeys, "d")
	checkSliceContains(t, aKeys, "e")
	checkSliceContains(t, aKeys, "f")
}

func TestObjectGetElm(t *testing.T) {
	o := wrap.Object(map[string]wrap.JsonValue{
		"a": wrap.Null(),
		"b": wrap.Number(123),
		"c": wrap.String("abc"),
		"d": wrap.Boolean(true),
		"e": wrap.Object(),
		"f": wrap.Array(),
	})

	t.Run("get null", func(t *testing.T) {
		v, ok := o.ObjectGetElm("a")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})

	t.Run("get number", func(t *testing.T) {
		v, ok := o.ObjectGetElm("b")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
	})

	t.Run("get string", func(t *testing.T) {
		v, ok := o.ObjectGetElm("c")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeString)
	})

	t.Run("get boolean", func(t *testing.T) {
		v, ok := o.ObjectGetElm("d")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeBoolean)
	})

	t.Run("get object", func(t *testing.T) {
		v, ok := o.ObjectGetElm("e")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeObject)
	})

	t.Run("get array", func(t *testing.T) {
		v, ok := o.ObjectGetElm("f")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeArray)
	})
}

func TestObjectSetElm(t *testing.T) {
	t.Run("set null", func(t *testing.T) {
		o := wrap.Object()
		o.ObjectSetElm("a", wrap.Null())
		v, ok := o.ObjectGetElm("a")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})

	t.Run("set number", func(t *testing.T) {
		o := wrap.Object()
		o.ObjectSetElm("b", wrap.Number(123))
		v, ok := o.ObjectGetElm("b")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
	})

	t.Run("set string", func(t *testing.T) {
		o := wrap.Object()
		o.ObjectSetElm("c", wrap.String("abc"))
		v, ok := o.ObjectGetElm("c")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeString)
	})

	t.Run("set boolean", func(t *testing.T) {
		o := wrap.Object()
		o.ObjectSetElm("d", wrap.Boolean(true))
		v, ok := o.ObjectGetElm("d")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeBoolean)
	})

	t.Run("set object", func(t *testing.T) {
		o := wrap.Object()
		o.ObjectSetElm("e", wrap.Object())
		v, ok := o.ObjectGetElm("e")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeObject)
	})

	t.Run("set array", func(t *testing.T) {
		o := wrap.Object()
		o.ObjectSetElm("f", wrap.Array())
		v, ok := o.ObjectGetElm("f")
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeArray)
	})
}

func TestObjectDelElm(t *testing.T) {
	t.Run("delete null", func(t *testing.T) {
		o := wrap.Object(map[string]wrap.JsonValue{"a": wrap.Null()})
		o.ObjectDelElm("a")
		_, ok := o.ObjectGetElm("a")
		assert.Equal(t, ok, false)
	})

	t.Run("delete number", func(t *testing.T) {
		o := wrap.Object(map[string]wrap.JsonValue{"b": wrap.Number(123)})
		o.ObjectDelElm("b")
		_, ok := o.ObjectGetElm("b")
		assert.Equal(t, ok, false)
	})

	t.Run("delete string", func(t *testing.T) {
		o := wrap.Object(map[string]wrap.JsonValue{"c": wrap.String("abc")})
		o.ObjectDelElm("c")
		_, ok := o.ObjectGetElm("c")
		assert.Equal(t, ok, false)
	})

	t.Run("delete boolean", func(t *testing.T) {
		o := wrap.Object(map[string]wrap.JsonValue{"d": wrap.Boolean(true)})
		o.ObjectDelElm("d")
		_, ok := o.ObjectGetElm("d")
		assert.Equal(t, ok, false)
	})

	t.Run("delete object", func(t *testing.T) {
		o := wrap.Object(map[string]wrap.JsonValue{"e": wrap.Object()})
		o.ObjectDelElm("e")
		_, ok := o.ObjectGetElm("e")
		assert.Equal(t, ok, false)
	})

	t.Run("delete array", func(t *testing.T) {
		o := wrap.Object(map[string]wrap.JsonValue{"f": wrap.Array()})
		o.ObjectDelElm("f")
		_, ok := o.ObjectGetElm("f")
		assert.Equal(t, ok, false)
	})
}

func TestObjectLen(t *testing.T) {
	o := wrap.Object(map[string]wrap.JsonValue{
		"a": wrap.Null(),
		"b": wrap.Number(123),
		"c": wrap.String("abc"),
		"d": wrap.Boolean(true),
		"e": wrap.Object(),
		"f": wrap.Array(),
	})

	assert.Equal(t, o.ObjectLen(), 6)
}

func TestArrayGetElm(t *testing.T) {
	o := wrap.Array(
		wrap.Null(),
		wrap.Number(123),
		wrap.String("abc"),
		wrap.Boolean(true),
		wrap.Object(),
		wrap.Array(),
	)

	t.Run("get null", func(t *testing.T) {
		v, ok := o.ArrayGetElm(0)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})

	t.Run("get number", func(t *testing.T) {
		v, ok := o.ArrayGetElm(1)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
	})

	t.Run("get string", func(t *testing.T) {
		v, ok := o.ArrayGetElm(2)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeString)
	})

	t.Run("get boolean", func(t *testing.T) {
		v, ok := o.ArrayGetElm(3)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeBoolean)
	})

	t.Run("get object", func(t *testing.T) {
		v, ok := o.ArrayGetElm(4)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeObject)
	})

	t.Run("get array", func(t *testing.T) {
		v, ok := o.ArrayGetElm(5)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeArray)
	})
}

func newExampleArray(n int, v wrap.JsonValue) wrap.JsonValue {
	var a = make([]wrap.JsonValue, n)
	for i := 0; i < n; i++ {
		a[i] = v
	}
	return wrap.Array(a...)
}
func TestArraySetElm(t *testing.T) {
	t.Run("get null", func(t *testing.T) {
		o := newExampleArray(6, wrap.Array())
		ok := o.ArraySetElm(0, wrap.Null())
		assert.Equal(t, ok, true)
		v, ok := o.ArrayGetElm(0)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})

	t.Run("get number", func(t *testing.T) {
		o := newExampleArray(6, wrap.Null())
		ok := o.ArraySetElm(1, wrap.Number(123))
		assert.Equal(t, ok, true)
		v, ok := o.ArrayGetElm(1)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
	})

	t.Run("get string", func(t *testing.T) {
		o := newExampleArray(6, wrap.Null())
		ok := o.ArraySetElm(2, wrap.String("abc"))
		assert.Equal(t, ok, true)
		v, ok := o.ArrayGetElm(2)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeString)
	})

	t.Run("get boolean", func(t *testing.T) {
		o := newExampleArray(6, wrap.Null())
		ok := o.ArraySetElm(3, wrap.Boolean(true))
		assert.Equal(t, ok, true)
		v, ok := o.ArrayGetElm(3)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeBoolean)
	})

	t.Run("get object", func(t *testing.T) {
		o := newExampleArray(6, wrap.Null())
		ok := o.ArraySetElm(4, wrap.Object())
		assert.Equal(t, ok, true)
		v, ok := o.ArrayGetElm(4)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeObject)
	})

	t.Run("get array", func(t *testing.T) {
		o := newExampleArray(6, wrap.Null())
		ok := o.ArraySetElm(5, wrap.Array())
		assert.Equal(t, ok, true)
		v, ok := o.ArrayGetElm(5)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeArray)
	})
}

func TestArrayLen(t *testing.T) {
	o := wrap.Array(
		wrap.Null(),
		wrap.Number(123),
		wrap.String("abc"),
		wrap.Boolean(true),
		wrap.Object(),
		wrap.Array(),
	)

	assert.Equal(t, o.ArrayLen(), 6)
}

func TestArrayAddElm(t *testing.T) {
	o := wrap.Array()

	o.ArrayAddElm(wrap.Null())
	o.ArrayAddElm(wrap.Number(123))
	o.ArrayAddElm(wrap.String("abc"))
	o.ArrayAddElm(wrap.Boolean(true))
	o.ArrayAddElm(wrap.Object())
	o.ArrayAddElm(wrap.Array())

	t.Run("get null", func(t *testing.T) {
		v, ok := o.ArrayGetElm(0)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNull)
	})

	t.Run("get number", func(t *testing.T) {
		v, ok := o.ArrayGetElm(1)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeNumber)
	})

	t.Run("get string", func(t *testing.T) {
		v, ok := o.ArrayGetElm(2)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeString)
	})

	t.Run("get boolean", func(t *testing.T) {
		v, ok := o.ArrayGetElm(3)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeBoolean)
	})

	t.Run("get object", func(t *testing.T) {
		v, ok := o.ArrayGetElm(4)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeObject)
	})

	t.Run("get array", func(t *testing.T) {
		v, ok := o.ArrayGetElm(5)
		assert.Equal(t, ok, true)
		assert.Equal(t, v.Type(), wrap.JsonTypeArray)
	})
}

func TestArraySlice(t *testing.T) {
	o := wrap.Array(
		wrap.Null(),
		wrap.Number(123),
		wrap.String("abc"),
		wrap.Boolean(true),
		wrap.Object(),
		wrap.Array(),
	)

	t.Run("whole", func(t *testing.T) {
		a, ok := o.ArraySlice(0, o.ArrayLen())
		assert.Equal(t, ok, true)
		assert.Equal(t, a.ArrayLen(), 6)
	})

	t.Run("empty", func(t *testing.T) {
		a, ok := o.ArraySlice(3, 3)
		assert.Equal(t, ok, true)
		assert.Equal(t, a.ArrayLen(), 0)
	})

	t.Run("sub-array", func(t *testing.T) {
		a, ok := o.ArraySlice(2, 4)
		assert.Equal(t, ok, true)
		assert.Equal(t, a.ArrayLen(), 2)
	})
}
