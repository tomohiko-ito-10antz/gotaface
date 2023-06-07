package dump

import "github.com/Jumpaku/gotaface/dml"

type Dumper interface {
	Dump(table string) (dml.Rows, error)
}
