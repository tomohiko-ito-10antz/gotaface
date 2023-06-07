package insert

import "github.com/Jumpaku/gotaface/dml"

type Inserter interface {
	Insert(table string, values dml.Rows) error
}
