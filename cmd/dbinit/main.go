package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Jumpaku/gotaface/cli/dbinit"

	_ "embed"
)

//go:embed README.md
var Usage string

func main() {
	cli := flag.NewFlagSet("gf-dbinit", flag.ExitOnError)
	cli.Usage = func() { fmt.Println(Usage) }
	schemaJSON := cli.String(`schema`, "", "specifies a path <schema-json> of a JSON-based schema file")

	if err := cli.Parse(os.Args[1:]); err != nil {
		log.Fatalf(`cannot parse command line arguments: %v`, err)
	}

	args := cli.Args()
	if len(args) != 2 {
		log.Fatalln(`positional arguments <driver> and <data-source> are required`)
	}

	driver, dataSource := args[0], args[1]
	runner, err := dbinit.BuildRunner(driver, dataSource, *schemaJSON)
	if err != nil {
		log.Fatalf(`fail to execute %v`, err)
	}

	ctx := context.Background()
	err = runner.Run(ctx, os.Stdin, os.Stdout)
	if err != nil {
		log.Fatalf(`failed execution %v`, err)
	}
}
