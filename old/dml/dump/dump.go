package dump

import (
	"context"

	"github.com/Jumpaku/gotaface/dml"
)

type Dumper interface {
	Dump(ctx context.Context, table string) (dml.Rows, error)
}
