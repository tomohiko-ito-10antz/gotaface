package insert

import (
	"context"

	"github.com/Jumpaku/gotaface/dml"
)

type Inserter interface {
	Insert(ctx context.Context, table string, values dml.Rows) error
}
