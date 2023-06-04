package assert

import (
	"math"
	"reflect"
	"testing"
)

func anyEqual(actual any, expect any) bool {
	return actual == expect
}
func Equal[T comparable](t *testing.T, actual T, expect T) {
	t.Helper()

	if !anyEqual(actual, expect) {
		t.Errorf("ASSERT EQUAL\n  expect: %v:%T\n  actual: %v:%T", expect, expect, actual, actual)
	}
}

func NotEqual[T comparable](t *testing.T, actual T, expect T) {
	t.Helper()

	if anyEqual(actual, expect) {
		t.Errorf("ASSERT NOT EQUAL\n  expect: %v:%T\n  actual: %v:%T", expect, expect, actual, actual)
	}
}

func isNil(value any) bool {
	rv := reflect.ValueOf(value)

	if !rv.IsValid() {
		return true
	}

	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	}

	return false
}
func IsNil(t *testing.T, actual any) {
	t.Helper()

	if isNil(actual) {
		t.Errorf("ASSERT IS NIL\n  actual: %v:%T", actual, actual)
	}
}

func IsNotNil(t *testing.T, actual any) {
	t.Helper()

	if isNil(actual) {
		t.Errorf("ASSERT IS NOT NIL\n  actual: %v:%T", actual, actual)
	}
}

func anyCloseTo(actual any, expect any, tolerance any) bool {
	var a float64
	var e float64
	var t float64
	switch actual := actual.(type) {
	case float32:
		a = float64(actual)
	case float64:
		a = float64(actual)
	}
	switch expect := tolerance.(type) {
	case float32:
		e = float64(expect)
	case float64:
		e = float64(expect)
	}
	switch tolerance := tolerance.(type) {
	case float32:
		t = float64(tolerance)
	case float64:
		t = float64(tolerance)
	}
	return math.Abs(a-e) <= t
}

func CloseTo[T float32 | float64](t *testing.T, actual T, expect T, tolerance T) {
	t.Helper()

	if anyCloseTo(actual, expect, tolerance) {
		t.Errorf("ASSERT CLOSE TO\n   expect: %v\n    actual: %v\n tolerance: %v", expect, actual, tolerance)
	}
}

func NotCloseTo[T float32 | float64](t *testing.T, actual T, expect T, tolerance T) {
	t.Helper()

	if anyCloseTo(actual, expect, tolerance) {
		t.Errorf("ASSERT NOT CLOSE TO\n   expect: %v\n    actual: %v\n tolerance: %v", expect, actual, tolerance)
	}
}
func Match[T any](t *testing.T, actual T, matcher func(actual T) (matches bool, message string)) {
	t.Helper()

	if matches, message := matcher(actual); !matches {
		t.Errorf("ASSERT MATCH\n  expect: '%v' is true\n  actual: %v:%T", message, actual, actual)
	}
}

func NotMatch[T any](t *testing.T, actual T, matcher func(actual T) (matches bool, message string)) {
	t.Helper()

	if matches, message := matcher(actual); matches {
		t.Errorf("ASSERT NOT MATCH\n  expect: '%v' is false\n  actual: %v:%T", message, actual, actual)
	}
}
