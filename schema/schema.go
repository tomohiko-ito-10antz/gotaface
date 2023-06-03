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
}

type Schema interface {
	Tables() []Table
	References() [][]int
}

type Fetcher interface {
	Fetch(ctx context.Context) (Schema, error)
}
