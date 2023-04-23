package dbproxy

import (
	"fmt"
	"io"
)

const (
	driver_name = "dbproxy"
)

type Result struct {
	lastInsertId int64
	rowsAffected int64
}

func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type ResultSet struct {
	columns []string
	rows <-chan []interface{}
}

func (rs *ResultSet) Columns() []string {
	return rs.columns
}

func (rs *ResultSet) Close() error {
	return nil
}

func (rs *ResultSet) Next(dest []interface{}) error {
	if rs.rows == nil {
		return io.EOF
	}

	row, ok := <-rs.rows
	if !ok {
		return io.EOF
	}
	if len(row) != len(dest) {
		return fmt.Errorf("different columns")
	}

	for i, col := range row {
		dest[i] = col
	}
	return nil
}

