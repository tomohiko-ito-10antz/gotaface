package spanner

import (
	"fmt"

	"cloud.google.com/go/spanner"
)

func ScanRowsStruct[RowStruct any](itr *spanner.RowIterator) ([]RowStruct, error) {
	var rows []RowStruct
	err := itr.Do(func(r *spanner.Row) error {
		var row RowStruct
		err := r.ToStructLenient(&row)
		if err != nil {
			return fmt.Errorf(`fail to scan row: %w`, err)
		}
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf(`fail to scan rows: %w`, err)
	}
	return rows, err
}
