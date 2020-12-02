package sqlm

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

// TableOutput implement write support for db table
type TableOutput struct {
	backend TableFuncInterface
}

// Write implement io.Writer
func (o TableOutput) Write(data []byte) (int, error) {
	if o.backend == nil {
		return 0, errors.New("backend not setted")
	}
	return Write(o.backend, data)
}

// NewTableOutput return write device with db table backend.
func NewTableOutput(t TableFuncInterface) io.Writer {
	return TableOutput{t}
}

// Write common write logic for table backend
func Write(t TableFuncInterface, p []byte) (int, error) {
	// TODO: 后续考虑更佳性能的序列化和反序列化方案
	record := t.RowModel()
	if err := json.Unmarshal(p, record); err != nil {
		return 0, err
	}
	if _, err := t.Insert(record); err != nil {
		return 0, err
	}
	return len(p), nil
}
