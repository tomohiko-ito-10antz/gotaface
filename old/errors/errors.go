package errors

import "fmt"

// Assert panics with formatted message if asserted condition is false
func Assert(condition bool, format string, args ...any) {
	if !condition {
		panic(fmt.Errorf(`assertion failed: `+format, args...))
	}
}

// Require panics with formatted message if required condition is false
func Require(ok bool, format string, args ...any) {
	if !ok {
		panic(fmt.Errorf(`requirement failed: `+format, args...))
	}
}

// Unexpected panics with formatted message, which represents entering into unexpected code path.
func Unexpected(format string, args ...any) {
	panic(fmt.Errorf(`unexpected state: `+format, args...))
}

// Unexpected1 panics with formatted message, which represents entering into unexpected code path. This function can be called as a value to be returned.
func Unexpected1[T any](format string, args ...any) T {
	panic(fmt.Errorf(`unexpected state: `+format, args...))
}

// Unexpected2 panics with formatted message, which represents entering into unexpected code path. This function can be called as values to be returned.
func Unexpected2[T1, T2 any](format string, args ...any) (T1, T2) {
	panic(fmt.Errorf(`unexpected state: `+format, args...))
}

// Unexpected3 panics with formatted message, which represents entering into unexpected code path. This function can be called as values to be returned.
func Unexpected3[T1, T2, T3 any](format string, args ...any) (T1, T2, T3) {
	panic(fmt.Errorf(`unexpected state: `+format, args...))
}
