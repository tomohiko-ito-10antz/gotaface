# gf-dbschema

gf-dbschema is a command-line tool designed to simplify table initialization. It supports drivers for Spanner and SQLite3 and provides a standardized interface for deleting existing data and inserting new rows. By utilizing a JSON-based input format and providing consistent functionality across data sources, it simplifies the task of defining and managing the data to be inserted.

## Usage

```sh
gf-dbschema <driver> <data-source>
gf-dbschema -h | --help
```

gf-dbschema is a command-line tool used to fetch schema information of the tables in a data source. It supports the following drivers:

- Spanner
- SQLite3

To use gf-dbschema with Spanner, specify `spanner` as the `<driver>` and provide a string in the format `projects/<project>/instances/<instance>/databases/<database>` as the `<data-source>`. In this format, `<project>` represents the name of your Google Cloud Platform (GCP) project, `<instance>` is the name of your Spanner instance in the GCP project, and `<database>` is the name of the database within the Spanner instance.

To use gf-dbschema with SQLite3, set `sqlite3` as the `<driver>` and provide a connection string as the `<data-source>`.
The connection string should follow the format described in [https://github.com/mattn/go-sqlite3#connection-string](https://github.com/mattn/go-sqlite3#connection-string), such as `file:test.db?cache=shared&mode=memory`.

## Input

No specific input is required.

## Output

gf-dbschema outputs the schema information of the tables to stdout in JSON format. The schema information is represented as a structure that is assignable to the following structure `DBSchemaOutput`:

```ts
type DBSchemaOutput = {
    tables: Table[]; // array of information of tables
    references: number[][]; // "references[x] is [a, b]" means that tables[x] references tables[a] and tables[b]
}
type Table = {
    name: string; // name of a table
    columns: Column[]; // column information of the table
    primary_key: number[]; // ordered primary key indices of the columns
}
type Column = {
    name: string; // name of a column
    type: string; // type of the column
}
```

Here's an example:
```json
{
	"tables":[
		{
			"name": "t0",
			"columns": [
				{ "name": "id1", "type": "INT" },
				{ "name": "id2", "type": "INT" },
				{ "name": "col_integer", "type": "INTEGER" },
				{ "name": "col_text", "type": "TEXT" },
				{ "name": "col_real", "type": "REAL" },
				{ "name": "col_blob", "type": "BLOB" }
			],
			"primary_key": [0, 1]
		}, {
			"name": "t1",
			"columns": [
				{ "name": "id", "type": "INT" }
			],
			"primary_key": [0]
		}, {
			"name": "t2",
			"columns": [
				{ "name": "id", "type": "INT" }
			],
			"primary_key": [0]
		}, {
			"name": "t3",
			"columns": [
				{ "name": "id", "type": "INT" },
				{ "name": "col1", "type": "INT" },
				{ "name": "col2", "type": "INT" }
			],
			"primary_key": [0]
		}, {
			"name": "t4",
			"columns": [
				{ "name": "id", "type": "INT" }
			],
			"primary_key": [0]
		}, {
			"name": "t5",
			"columns": [
				{ "name": "id1", "type": "INT" },
				{ "name": "id2", "type": "INT" }
			],
			"primary_key": [0, 1]
		}, {
			"name": "t6",
			"columns": [
				{ "name": "id1", "type": "INT" },
				{ "name": "id2", "type": "INT" },
				{ "name": "id3", "type": "INT" }
			],
			"primary_key": [0, 1, 2]
		}
	],
	"references": [[], [0], [0], [1, 2], [], [4], [5]]
}
```

This example implies:

- a table named `t1` is in the data source.
- `t1` contains columns `id1` of type `INT`, `id2` of type `INT`, `col_integer` of type `INTEGER`, `col_text` of type `TEXT`, `col_real` of type `REAL`, and `col_blob` of type `BLOB`.
- primary key of `t1` is the pair of 0th column and 1st column, i.e. `(id1, id2)`
- 0th table is referenced by only 1st and 2nd tables, i.e. `t1` and `t2` have foreign keys referencing to `t0`.
- etc.
