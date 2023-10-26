package cli

import (
	"context"
	"io"
)

type Runner interface {
	Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error
}
