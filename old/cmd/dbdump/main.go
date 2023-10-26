package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Jumpaku/gotaface/old/dml"
	dbdump_spanner "github.com/Jumpaku/gotaface/old/spanner/cli/dbdump"
	dbdump_sqlite3 "github.com/Jumpaku/gotaface/old/sqlite3/cli/dbdump"
)

//go:embed README.md
var Usage string

func main() {
	cmd := flag.NewFlagSet("gf-dbdump", flag.ExitOnError)
	cmd.Usage = func() { fmt.Println(Usage) }

	schema := cmd.String(`schema`, `.gf-schema.json`, `path of schema cache file`)

	if err := cmd.Parse(os.Args[1:]); err != nil {
		log.Fatalf(`cannot parse command line arguments: %v`, err)
	}

	schemaReader, err := LoadSchemaCache(*schema)
	if err != nil {
		log.Fatalf(`fail to load schema cache: %v`, err)
	}

	var schemaWriter io.Writer
	if schemaReader == nil {
		f, err := os.Create(*schema)
		if err != nil {
			log.Fatalf(`fail to open schema cache: %v`, err)
		}
		defer f.Close()

		schemaWriter = f
	}

	args := cmd.Args()
	if len(args) != 2 {
		log.Fatalln(`positional arguments <driver> and <data-source> are required`)
	}

	err = Runner{driver: args[0], dataSource: args[1], schemaReader: schemaReader, schemaWriter: schemaWriter}.Run(context.Background(), os.Stdin, os.Stdout)
	if err != nil {
		log.Fatalf(`failed execution: %v`, err)
	}
}

type DBDumpInput = []string
type DBDumpOutput = map[string]dml.Rows
type DBDumpFunc func(ctx context.Context, driver string, dataSource string, schemaReader io.Reader, schemaWriter io.Writer, input DBDumpInput) (DBDumpOutput, error)

type Runner struct {
	driver       string
	dataSource   string
	schemaReader io.Reader
	schemaWriter io.Writer
}

func LoadSchemaCache(schema string) (io.Reader, error) {
	fi, err := os.Stat(schema)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf(`fail to open %s: %w`, schema, err)
	} else if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if fi.IsDir() {
		return nil, fmt.Errorf(`%s must be a file`, schema)
	}

	f, err := os.Open(schema)
	if err != nil {
		return nil, fmt.Errorf(`fail to open %s: %w`, schema, err)
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf(`fail to read %s: %w`, schema, err)
	}

	return bytes.NewBuffer(b), nil
}

func (runner Runner) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	var dbDumpFunc DBDumpFunc

	switch runner.driver {
	default:
		return fmt.Errorf(`unsupported driver %s`, runner.driver)
	case `spanner`:
		dbDumpFunc = dbdump_spanner.DBDumpFunc
	case `sqlite3`:
		dbDumpFunc = dbdump_sqlite3.DBDumpFunc
	}

	var input DBDumpInput
	d := json.NewDecoder(stdin)
	d.DisallowUnknownFields()
	if err := d.Decode(&input); err != nil {
		return fmt.Errorf(`fail to decode JSON from stdin`)
	}

	output, err := dbDumpFunc(ctx, runner.driver, runner.dataSource, runner.schemaReader, runner.schemaWriter, input)
	if err != nil {
		return fmt.Errorf(`fail to execute dbdump`)
	}

	e := json.NewEncoder(stdout)
	if err := e.Encode(output); err != nil {
		return fmt.Errorf(`fail to encode JSON to stdout`)
	}

	return nil
}
