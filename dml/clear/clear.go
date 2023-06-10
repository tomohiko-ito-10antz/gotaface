package clear

import "context"

type Clearer interface {
	Clear(ctx context.Context, table string) error
}
