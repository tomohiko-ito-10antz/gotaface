package clear

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml/clear"
	gotaface_spanner "github.com/Jumpaku/gotaface/spanner"
)

type clearer struct {
	updater gotaface_spanner.Updater
}

var _ clear.Clearer = clearer{}

func NewClearer(updater gotaface_spanner.Updater) clearer {
	return clearer{updater: updater}
}

func (clearer clearer) Clear(ctx context.Context, table string) error {
	_, err := clearer.updater.Update(ctx, spanner.Statement{SQL: fmt.Sprintf(`DELETE FROM %s`, table)})
	if err != nil {
		return fmt.Errorf(`fail to clear table: %w`, err)
	}
	return nil
}
