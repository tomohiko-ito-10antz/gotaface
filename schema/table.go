package schema

import (
	"context"
	"reflect"
)

type Column interface {
	Name() string
	Type() string
	GoType() reflect.Type
}

type Table interface {
	Name() string
	Columns() []Column
	PrimaryKey() []int
	References() []Table
}

type Lister interface {
	List(ctx context.Context) []Table
}
