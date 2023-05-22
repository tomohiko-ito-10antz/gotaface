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
	rv := reflect.ValueOf(s)
	if rv.IsValid() || rv.Kind() != reflect.Struct {
		panic("generic type must be struct")
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
		rvFieldValuePtr := reflect.New(rvField.Type())

		if !columnValue.AssignTo(rvFieldValuePtr.Interface()) {
			continue
		}
		if !rvFieldValuePtr.Elem().Type().AssignableTo(rvField.Type()) {
			continue
		}
		rvField.Set(rvFieldValuePtr.Elem())
	}

	return s
}

func ScanRows(rows *sql.Rows, scanTypes ScanRowTypes) ([]ScanRowValue, error) {
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("fail to scan row values: %w", err)
	}
	rowValues := []ScanRowValue{}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i, column := range columns {
			scanType, ok := scanTypes[column.Name()]
			if !ok {
				return nil, fmt.Errorf("fail to scan row Any %s", column.Name())
			}

			rv := reflect.New(scanType)
			values[i] = rv.Elem().Interface()
			pointers[i] = &values[i]
		}

		err := rows.Scan(pointers...)
		if err != nil {
			return nil, fmt.Errorf("fail to scan row values: %w", err)
		}

		rowValue := map[string]any{}
		for i, column := range columns {
			rowValue[column.Name()] = values[i]
		}

		rowValues = append(rowValues, rowValue)
	}
	return rowValues, nil
}
