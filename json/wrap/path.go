package wrap

import (
	"strconv"

	"github.com/Jumpaku/gotaface/errors"
	"golang.org/x/exp/slices"
)

type Key string

func (e Key) String() string {
	return string(e)
}
func (e Key) Integer() int {
	elm, err := strconv.ParseInt(e.String(), 10, 64)
	if err != nil {
		errors.Unexpected("cannot parse %v to int: %w", e, err)
	}
	return int(elm)
}

type Path []Key

func (k Path) Equals(other Path) bool {
	return slices.Equal(k, other)
}
func (k Path) Len() int {
	return len(k)
}
func (k Path) Append(key Key) Path {
	return Path(append(k, key))
}

func (k Path) Get(index int) Key {
	errors.Assert(0 <= index && index < len(k), "index must be in [0, %d)", len(k))

	return k[index]
}

func Walk(v JsonValue, visitor func(path Path, val JsonValue) error) error {
	return walkImpl(Path{}, v, visitor)
}

func walkImpl(parentKey Path, val JsonValue, walkFunc func(key Path, val JsonValue) error) error {
	if err := walkFunc(parentKey, val); err != nil {
		return err
	}
	switch val.Type() {
	case JsonTypeObject:
		for _, key := range val.ObjectKeys() {
			val := val.ObjectGetElm(key)
			if err := walkImpl(parentKey.Append(Key(key)), val, walkFunc); err != nil {
				return err
			}
		}
	case JsonTypeArray:
		for i := 0; i < val.ArrayLen(); i++ {
			val := val.ArrayGetElm(i)
			if err := walkImpl(parentKey.Append(Key(strconv.FormatInt(int64(i), 10))), val, walkFunc); err != nil {
				return err
			}
		}
	}
	return nil
}

func Find(v JsonValue, path Path) (JsonValue, bool) {
	var found JsonValue
	_ = Walk(v, func(p Path, val JsonValue) error {
		if p.Equals(path) {
			found = val
		}
		return nil
	})

	return found, found != nil
}
