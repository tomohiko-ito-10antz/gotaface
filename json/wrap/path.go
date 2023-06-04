package wrap

import (
	"strconv"

	"golang.org/x/exp/slices"
)

type Key string

func (e Key) String() string {
	return string(e)
}
func (e Key) Integer() (int, bool) {
	elm, err := strconv.ParseInt(e.String(), 10, 64)
	return int(elm), err == nil
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

func (k Path) Get(index int) (Key, bool) {
	if index >= k.Len() {
		return "", false
	}
	if index < 0 {
		return "", false
	}
	return k[index], true
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
			val, _ := val.ObjectGetElm(key)
			if err := walkImpl(parentKey.Append(Key(key)), val, walkFunc); err != nil {
				return err
			}
		}
	case JsonTypeArray:
		for i := 0; i < val.ArrayLen(); i++ {
			val, _ := val.ArrayGetElm(i)
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
