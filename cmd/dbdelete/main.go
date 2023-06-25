package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	dbdelete_spanner "github.com/Jumpaku/gotaface/spanner/cli/dbdelete"

	_ "embed"
)

//go:embed README.md
var Usage string

func main() {
	cmd := flag.NewFlagSet("gf-dbdelete", flag.ExitOnError)
	cmd.Usage = func() { fmt.Println(Usage) }
	schemaJSON := cmd.String(`schema`, ".gf-schema.json", "specifies a path <schema-json> of a JSON-based schema file")

	if err := cmd.Parse(os.Args[1:]); err != nil {
		log.Fatalf(`cannot parse command line arguments: %v`, err)
	}

	args := cmd.Args()
	if len(args) != 2 {
		log.Fatalln(`positional arguments <driver> and <data-source> are required`)
	}

	err := Runner{driver: args[0], dataSource: args[1], schemaJSON: *schemaJSON}.Run(context.Background(), os.Stdin, os.Stdout)
	if err != nil {
		log.Fatalf(`failed execution: %v`, err)
	}
}

type DBDeleteInput = []string
type DBDeleteFunc func(ctx context.Context, driver string, dataSource string, schemaReader io.Reader, schemaWriter io.Writer, input DBDeleteInput) error

type Runner struct {
	driver     string
	dataSource string
	schemaJSON string
}

func LoadSchemaJSON(schemaJSON string) (io.Reader, error) {
	fi, err := os.Stat(schemaJSON)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf(`fail to open %s: %w`, schemaJSON, err)
	} else if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if fi.IsDir() {
		return nil, fmt.Errorf(`%s must be a file`, schemaJSON)
	}

	schemaFile, err := os.Open(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf(`fail to open %s: %w`, schemaJSON, err)
	}
	defer schemaFile.Close()

	b, err := io.ReadAll(schemaFile)
	if err != nil {
		return nil, fmt.Errorf(`fail to read %s: %w`, schemaJSON, err)
	}
	return bytes.NewBuffer(b), nil
}
func (runner Runner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	var dbDeleteFunc DBDeleteFunc

	switch runner.driver {
	default:
		return fmt.Errorf(`unsupported driver %s`, runner.driver)
	case `spanner`:
		dbDeleteFunc = dbdelete_spanner.DBDeleteFunc
	case `sqlite3`:
		//	dbDeleteFunc = dbdelete_sqlite.DBDeleteFunc
	}

	var input DBDeleteInput
	d := json.NewDecoder(stdin)
	d.DisallowUnknownFields()
	if err := d.Decode(&input); err != nil {
		return fmt.Errorf(`fail to decode JSON from stdin`)
	}

	schemaReader, err := LoadSchemaJSON(runner.schemaJSON)
	if err := d.Decode(&input); err != nil {
		return fmt.Errorf(`fail to load schema from %s`, runner.schemaJSON)
	}

	schemaWriter, err := os.OpenFile(runner.schemaJSON, os.O_CREATE, 066)
	if err := d.Decode(&input); err != nil {
		return fmt.Errorf(`fail to load schema from %s`, runner.schemaJSON)
	}
	defer schemaWriter.Close()
	err = dbDeleteFunc(ctx, runner.driver, runner.dataSource, schemaReader, schemaWriter, input)
	if err != nil {
		return fmt.Errorf(`fail to execute dbdelete`)
	}

	return nil
}
