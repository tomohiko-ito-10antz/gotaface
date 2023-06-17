package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

type SchemaFormat struct {
	TablesVal     []TableFormat `json:"tables"`
	ReferencesVal [][]int       `json:"references"`
}

var _ Schema = SchemaFormat{}

type TableFormat struct {
	NameVal       string         `json:"name"`
	ColumnsVal    []ColumnFormat `json:"columns"`
	PrimaryKeyVal []int          `json:"primary_key"`
}
type ColumnFormat struct {
	NameVal string `json:"name"`
	TypeVal string `json:"type"`
}

func (sf SchemaFormat) Tables() []Table {
	tables := []Table{}
	for _, table := range sf.TablesVal {
		tables = append(tables, table)
	}
	return tables
}

func (sf SchemaFormat) References() [][]int {
	return [][]int(sf.ReferencesVal)
}

func (tf TableFormat) Name() string {
	return tf.NameVal
}

func (tf TableFormat) PrimaryKey() []int {
	return tf.PrimaryKeyVal
}

func (tf TableFormat) Columns() []Column {
	columns := []Column{}
	for _, column := range tf.ColumnsVal {
		columns = append(columns, column)
	}
	return columns
}

func (cf ColumnFormat) Name() string {
	return cf.NameVal
}
func (cf ColumnFormat) Type() string {
	return cf.TypeVal
}

func (sf *SchemaFormat) UnmarshalJSON(b []byte) error {
	br := bytes.NewBuffer(b)
	decoder := json.NewDecoder(br)
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	type SchemaFormatWrapper SchemaFormat
	var schema SchemaFormatWrapper
	if err := decoder.Decode(&schema); err != nil {
		return fmt.Errorf(`fail to decode JSON-based schema: %w`, err)
	}
	*sf = SchemaFormat(schema)
	return nil
}

func (sf *SchemaFormat) MarshalJSON() ([]byte, error) {
	bw := bytes.NewBuffer(nil)
	encoder := json.NewEncoder(bw)
	type SchemaFormatWrapper SchemaFormat
	if err := encoder.Encode(SchemaFormatWrapper(*sf)); err != nil {
		return nil, fmt.Errorf(`fail to encode JSON-based schema: %w`, err)
	}

	return bw.Bytes(), nil
}

type jsonFetcher struct{ ByteArray []byte }

var _ Fetcher = jsonFetcher{}

func NewFetcher(b []byte) jsonFetcher {
	return jsonFetcher{ByteArray: b}
}

func (fetcher jsonFetcher) Fetch(ctx context.Context) (Schema, error) {
	var schema SchemaFormat
	if err := schema.UnmarshalJSON(fetcher.ByteArray); err != nil {
		return nil, fmt.Errorf(`fail to unmarshal JSON-based schema data: %w`, err)
	}
	return schema, nil
}

func ReadSchema(reader io.Reader) (Schema, error) {
	b, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf(`fail to read schema data: %w`, err)
	}
	var schema SchemaFormat
	if err := schema.UnmarshalJSON(b); err != nil {
		return nil, fmt.Errorf(`fail to unmarshal JSON-based schema data: %w`, err)
	}
	return schema, nil
}

func WriteSchema(schema Schema, writer io.Writer) error {
	schemaFormat := SchemaFormat{ReferencesVal: schema.References()}
	for _, table := range schema.Tables() {
		tableFormat := TableFormat{
			NameVal:       table.Name(),
			PrimaryKeyVal: table.PrimaryKey(),
		}
		for _, column := range table.Columns() {
			tableFormat.ColumnsVal = append(tableFormat.ColumnsVal, ColumnFormat{
				NameVal: column.Name(),
				TypeVal: column.Type(),
			})
		}

		schemaFormat.TablesVal = append(schemaFormat.TablesVal, tableFormat)
	}

	b, err := schemaFormat.MarshalJSON()
	if err != nil {
		return fmt.Errorf(`fail to marshal JSON-based schema data: %w`, err)
	}
	if _, err := writer.Write(b); err != nil {
		return fmt.Errorf(`fail to write schema data: %w`, err)
	}

	return nil
}
