package main

import (
	_ "embed"
)

//go:embed README.md
var Usage string

func main() {
	/*
		cmd := flag.NewFlagSet("gf-dbinit", flag.ExitOnError)
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
		var fetcher schema.Fetcher
		if schemaJSON := *schemaJSON; schemaJSON != "" {
			f, err := os.Open(schemaJSON)
			if err != nil {
				log.Fatalf(`fail to open file: %s: %v`, schemaJSON, err)
			}

			b, err := io.ReadAll(f)
			if err != nil {
				log.Fatalf(`fail to read file: %s : %v`, schemaJSON, err)
			}

			fetcher = schema.NewFetcher(b)
		}

		var runner cli.Runner
		switch driver {
		default:
			log.Fatalf(`unsupported driver %s`, driver)
		case `spanner`:
			//runner = &dbinit_spanner.SpannerRunner{DataSource: dataSource, Fetcher: fetcher}
		case `sqlite3`:
			//runner = &dbinit_sqlite.SqliteRunner{DataSource: dataSource, Fetcher: fetcher}
		}

		ctx := context.Background()
		err := runner.Run(ctx, os.Stdin, os.Stdout)
		if err != nil {
			log.Fatalf(`failed execution: %v`, err)
		}
	*/
}
