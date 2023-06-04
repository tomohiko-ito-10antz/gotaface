package errors

import "fmt"

func Assert(ok bool, format string, args ...any) {
	if !ok {
		panic(fmt.Errorf(`assertion failed: `+format, args...))
	}
}

func Unreachable[T any]() T {
	panic(fmt.Errorf(`unreachable`))
}

func Unreachable2[T1, T2 any]() (T1, T2) {
	panic(fmt.Errorf(`unreachable`))
}

func Unreachable3[T1, T2, T3 any]() (T1, T2, T3) {
	panic(fmt.Errorf(`unreachable`))
}
