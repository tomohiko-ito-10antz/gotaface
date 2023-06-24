package dbschema

import (
	"log"

	"github.com/Jumpaku/gotaface/cli"
	"github.com/Jumpaku/gotaface/ddl/schema"
	"github.com/Jumpaku/gotaface/errors"
	dbschema_spanner "github.com/Jumpaku/gotaface/spanner/cli/dbschema"
	dbschema_sqlite "github.com/Jumpaku/gotaface/sqlite/cli/dbschema"
)

type DBSchemaOutput = schema.SchemaFormat

func NewRunner(driver string, dataSource string) cli.Runner {
	switch driver {
	default:
		log.Fatalf(`unsupported driver %s`, driver)
	case `spanner`:
		return &dbschema_spanner.SpannerRunner{DataSource: dataSource}
	case `sqlite3`:
		return &dbschema_sqlite.SQLiteRunner{DataSource: dataSource}
	}
	return errors.Unreachable[cli.Runner]()
}
