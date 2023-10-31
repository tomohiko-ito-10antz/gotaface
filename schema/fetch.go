package schema

import "context"

type Fetcher[Schema any] interface {
	Fetch(ctx context.Context, table string) (Schema, error)
}
