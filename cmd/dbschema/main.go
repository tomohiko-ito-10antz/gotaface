package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Jumpaku/gotaface/cli"
	dbschema_spanner "github.com/Jumpaku/gotaface/spanner/cli/dbschema"
	dbschema_sqlite "github.com/Jumpaku/gotaface/sqlite/cli/dbschema"
)

//go:embed README.md
var Usage string

func main() {
	cmd := flag.NewFlagSet("gf-dbschema", flag.ExitOnError)
	cmd.Usage = func() { fmt.Println(Usage) }

	if err := cmd.Parse(os.Args[1:]); err != nil {
		log.Fatalf(`cannot parse command line arguments: %v`, err)
	}

	args := cmd.Args()
	if len(args) != 2 {
		log.Fatalln(`positional arguments <driver> and <data-source> are required`)
	}

	driver, dataSource := args[0], args[1]

	var runner cli.Runner
	switch driver {
	default:
		log.Fatalf(`unsupported driver %s`, driver)
	case `spanner`:
		runner = &dbschema_spanner.SpannerRunner{DataSource: dataSource}
	case `sqlite3`:
		runner = &dbschema_sqlite.SqliteRunner{DataSource: dataSource}
	}

	ctx := context.Background()
	err := runner.Run(ctx, os.Stdin, os.Stdout)
	if err != nil {
		log.Fatalf(`failed execution: %v`, err)
	}
}
