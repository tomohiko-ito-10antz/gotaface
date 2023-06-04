package errors_test

import (
	"testing"

	"github.com/Jumpaku/gotaface/errors"
)

func TestAssert_OK(t *testing.T) {
	defer func() {
		cause := recover()
		if cause != nil {
			t.Fatalf(`panic detected`)
		}
	}()
	errors.Assert(true, "does not panic")
}

func TestAssert_NG(t *testing.T) {
	defer func() {
		cause := recover()
		if cause == nil {
			t.Fatalf(`panic not detected`)
		}
		err, ok := cause.(error)
		if !ok {
			t.Fatalf(`panic not detected`)
		}
		got := err.Error()
		want := "assertion failed: due to some reason"
		if got != want {
			t.Fatalf(`error message mismatch: got = %#v want = %#v`, got, want)
		}
	}()
	errors.Assert(false, "due to %v", "some reason")
}
