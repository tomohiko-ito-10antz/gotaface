package dbschema

import (
	"fmt"

	"github.com/Jumpaku/gotaface/cli"
	dbschema_spanner "github.com/Jumpaku/gotaface/spanner/cli/dbschema"
	dbschema_sqlite "github.com/Jumpaku/gotaface/sqlite/cli/dbschema"
)

func BuildRunner(driver string, dataSource string) (cli.Runner, error) {
	switch driver {
	default:
		return nil, fmt.Errorf(`unsupported driver %s`, driver)
	case `spanner`:
		return &dbschema_spanner.SpannerRunner{DataSource: dataSource}, nil
	case `sqlite3`:
		return &dbschema_sqlite.SqliteRunner{DataSource: dataSource}, nil
	}
}
