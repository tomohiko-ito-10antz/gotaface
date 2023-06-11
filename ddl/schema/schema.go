package schema

import (
	"context"
)

type Column interface {
	Name() string
	Type() string
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
