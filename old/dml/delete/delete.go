package delete

import "context"

type Deleter interface {
	Delete(ctx context.Context, table string) error
}
