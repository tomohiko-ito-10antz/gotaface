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

	"github.com/Jumpaku/gotaface/dml"
	//dbinsert_spanner "github.com/Jumpaku/gotaface/spanner/cli/dbdump"
	//dbinsert_sqlite3 "github.com/Jumpaku/gotaface/sqlite3/cli/dbdump"
)

//go:embed README.md
var Usage string

func main() {
	cmd := flag.NewFlagSet("gf-dbinsert", flag.ExitOnError)
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

type DBInsertInput = []struct {
	Name string   `json:"name"`
	Rows dml.Rows `json:"rows"`
}
type DBInsertFunc func(ctx context.Context, driver string, dataSource string, schemaReader io.Reader, schemaWriter io.Writer, input DBInsertInput) error

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
	var dbInsertFunc DBInsertFunc

	switch runner.driver {
	default:
		return fmt.Errorf(`unsupported driver %s`, runner.driver)
	case `spanner`:
		//dbInsertFunc = dbdump_spanner.DBInsertFunc
	case `sqlite3`:
		//dbInsertFunc = dbdump_sqlite3.DBInsertFunc
	}

	var input DBInsertInput
	d := json.NewDecoder(stdin)
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(&input); err != nil {
		return fmt.Errorf(`fail to decode JSON from stdin`)
	}

	err := dbInsertFunc(ctx, runner.driver, runner.dataSource, runner.schemaReader, runner.schemaWriter, input)
	if err != nil {
		return fmt.Errorf(`fail to execute dbdump`)
	}

	return nil
}
