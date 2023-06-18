package dbschema

import (
	"fmt"

	"github.com/Jumpaku/gotaface/cli"
)

func BuildRunner(driver string, dataSource string) (cli.Runner, error) {

	switch driver {
	default:
		return nil, fmt.Errorf(`unsupported driver %s`, driver)
	case `spanner`:
		return &spannerRunner{dataSource: dataSource}, nil
	case `sqlite3`:
		return &sqliteRunner{dataSource: dataSource}, nil
	}
}
