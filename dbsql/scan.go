package dbsql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type ScanRowTypes map[string]reflect.Type

func NewScanRowTypes[Struct any]() ScanRowTypes {
	var s Struct
	rt := reflect.TypeOf(s)
	if rt == nil || rt.Kind() != reflect.Struct {
		panic("generic type must be struct")
	}
	scanRowTypes := ScanRowTypes{}
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		scanRowTypes[field.Name] = field.Type
	}
	return scanRowTypes
}

type ScanRowValue map[string]any

func StructScanRowValue[Struct any](scanRowValue ScanRowValue) Struct {
	var s Struct
	rv := reflect.ValueOf(&s).Elem()
	if rv.Kind() != reflect.Struct {
		panic(fmt.Sprintf("generic type must be struct but %v", rv.Type()))
	}
	rt := rv.Type()
	fieldNameMapper := map[string]string{}
	for i := 0; i < rt.NumField(); i++ {
		rf := rt.Field(i)
		if !rf.IsExported() {
			continue
		}
		fieldNameMapper[strings.ToLower(rf.Name)] = rf.Name
	}

	for columnName, columnValue := range scanRowValue {
		columnValue := Value{Any: columnValue}
		lowerName := strings.ToLower(columnName)
		fieldName, ok := fieldNameMapper[lowerName]
		if !ok {
			continue
		}
		rvField := rv.FieldByName(fieldName)

		rvFieldPtr := rvField.Addr()
		if err := columnValue.AssignTo(rvFieldPtr.Interface()); err != nil {
			panic(fmt.Errorf("cannot assign to field value: %w", err))
		}
	}

	return s
}

func ScanRows(rows *sql.Rows, scanTypes ScanRowTypes) ([]ScanRowValue, error) {
	lowerColumns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("fail to scan row values: %w", err)
	}
	for i, column := range lowerColumns {
		lowerColumns[i] = strings.ToLower(column)
	}

	lowerColumnScanType := ScanRowTypes{}
	for column, scanType := range scanTypes {
		lowerColumnScanType[strings.ToLower(column)] = scanType
	}

	rowValues := []ScanRowValue{}
	for rows.Next() {
		pointers := make([]any, len(lowerColumns))
		for i, column := range lowerColumns {
			scanType, ok := lowerColumnScanType[lowerColumns[i]]
			if !ok {
				return nil, fmt.Errorf("fail to scan row Any %s", column)
			}

			pointers[i] = reflect.New(scanType).Interface()
		}

		err := rows.Scan(pointers...)
		if err != nil {
			return nil, fmt.Errorf("fail to scan row values: %w", err)
		}

		rowValue := map[string]any{}
		for i, column := range lowerColumns {
			rowValue[column] = reflect.ValueOf(pointers[i]).Elem().Interface()
		}

		rowValues = append(rowValues, rowValue)
	}
	return rowValues, nil
}

func ScanRowsStruct[Struct any](rows *sql.Rows) ([]*Struct, error) {
	scanRowValues, err := ScanRows(rows, NewScanRowTypes[Struct]())
	if err != nil {
		return nil, fmt.Errorf("fail to scan row values: %w", err)
	}
	scanRowValueStructs := []*Struct{}
	for _, scanRowValue := range scanRowValues {
		scanRowValueStruct := StructScanRowValue[Struct](scanRowValue)
		scanRowValueStructs = append(scanRowValueStructs, &scanRowValueStruct)
	}
	return scanRowValueStructs, nil
}
