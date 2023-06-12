package delete

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/Jumpaku/gotaface/dml/delete"
	gotaface_spanner "github.com/Jumpaku/gotaface/spanner"
)

type deleter struct {
	updater gotaface_spanner.Updater
}

var _ delete.Deleter = deleter{}

func NewDeleter(updater gotaface_spanner.Updater) deleter {
	return deleter{updater: updater}
}

func (deleter deleter) Delete(ctx context.Context, table string) error {
	_, err := deleter.updater.Update(ctx, spanner.Statement{SQL: fmt.Sprintf(`DELETE FROM %s WHERE TRUE`, table)})
	if err != nil {
		return fmt.Errorf(`fail to delete table: %w`, err)
	}
	return nil
}
