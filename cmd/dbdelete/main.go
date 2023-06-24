package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Jumpaku/gotaface/cli"
	"github.com/Jumpaku/gotaface/errors"
	dbdelete_spanner "github.com/Jumpaku/gotaface/spanner/cli/dbdelete"

	_ "embed"
)

//go:embed README.md
var Usage string

func main() {
	cmd := flag.NewFlagSet("gf-dbdelete", flag.ExitOnError)
	cmd.Usage = func() { fmt.Println(Usage) }
	schemaJSON := cmd.String(`schema`, "", "specifies a path <schema-json> of a JSON-based schema file")

	if err := cmd.Parse(os.Args[1:]); err != nil {
		log.Fatalf(`cannot parse command line arguments: %v`, err)
	}

	args := cmd.Args()
	if len(args) != 2 {
		log.Fatalln(`positional arguments <driver> and <data-source> are required`)
	}

	driver, dataSource := args[0], args[1]
	runner := NewRunner(driver, dataSource, DBDeleteOptions{schema: *schemaJSON})
	ctx := context.Background()
	err := runner.Run(ctx, os.Stdin, os.Stdout)
	if err != nil {
		log.Fatalf(`failed execution: %v`, err)
	}
}

type DBDeleteInput []string
type DBDeleteOptions struct {
	schema string
}

func NewRunner(driver string, dataSource string, options DBDeleteOptions) cli.Runner {
	switch driver {
	default:
		log.Fatalf(`unsupported driver %s`, driver)
	case `spanner`:
		return &dbdelete_spanner.SpannerRunner{DataSource: dataSource}
	case `sqlite3`:
		//return &dbdelete_sqlite.SQLiteRunner{DataSource: dataSource}
	}
	return errors.Unreachable[cli.Runner]()
}
