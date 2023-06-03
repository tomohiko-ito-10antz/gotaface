package spanner

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner"
)

type Queryer interface {
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

func ScanRows[Struct any](itr *spanner.RowIterator) ([]*Struct, error) {
	var s Struct
	rv := reflect.ValueOf(&s).Elem()
	if rv.Kind() != reflect.Struct {
		panic(fmt.Sprintf("generic type must be struct but %v", rv.Type()))
	}

	rows := []*Struct{}
	err := itr.Do(func(r *spanner.Row) error {
		var row Struct
		if err := r.ToStructLenient(&row); err != nil {
			return err
		}
		rows = append(rows, &row)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf(`fail to scan row: %w`, err)
	}
	return rows, nil
}
