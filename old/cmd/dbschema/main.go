package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Jumpaku/gotaface/old/ddl/schema"
	dbschema_spanner "github.com/Jumpaku/gotaface/old/spanner/cli/dbschema"
	dbschema_sqlite3 "github.com/Jumpaku/gotaface/old/sqlite3/cli/dbschema"
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

	runner := Runner{driver: args[0], dataSource: args[1]}
	err := runner.Run(context.Background(), os.Stdin, os.Stdout)
	if err != nil {
		log.Fatalf(`failed execution: %v`, err)
	}
}

type DBSchemaOutput = interface {
	json.Marshaler
	schema.Schema
}
type DBSchemaFunc func(ctx context.Context, driver string, dataSource string) (DBSchemaOutput, error)

type Runner struct {
	driver     string
	dataSource string
}

func (runner Runner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	var bdSchemaFunc DBSchemaFunc

	switch runner.driver {
	default:
		return fmt.Errorf(`unsupported driver %s`, runner.driver)
	case `spanner`:
		bdSchemaFunc = dbschema_spanner.DBSchemaFunc
	case `sqlite3`:
		bdSchemaFunc = dbschema_sqlite3.DBSchemaFunc
	}

	o, err := bdSchemaFunc(ctx, runner.driver, runner.dataSource)
	if err != nil {
		return fmt.Errorf(`fail to execute dbschema: %w`, err)
	}

	b, err := o.MarshalJSON()
	if err != nil {
		return fmt.Errorf(`fail to marshal schema to JSON: %w`, err)
	}

	if _, err = stdout.Write(b); err != nil {
		return fmt.Errorf(`fail to output to stdout: %w`, err)
	}

	return nil
}
