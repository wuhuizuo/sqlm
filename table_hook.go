package sqlm

// InsertHookFunc hook for table insert record operation
type InsertHookFunc func(t *Table, record interface{}) error

// InsertsHookFunc hook for table inserts records operation
type InsertsHookFunc func(t *Table, records []interface{}) error

// SaveHookFunc hook for table single record update operation
type SaveHookFunc func(t *Table, record interface{}) error

// UpdateHookFunc hook for table records update operation
type UpdateHookFunc func(t *Table, rf RowFilter, parts map[string]interface{}) error

// DeleteHookFunc hook for table delete records operation
type DeleteHookFunc func(t *Table, rf RowFilter) error

// TableOperateHook hook for single kind operation
type TableOperateHook struct {
	Before []interface{}
	After  []interface{}
}

func (h *TableOperateHook) Merge(other *TableOperateHook) {
	if other == nil {
		return
	}

	h.Before = append(h.Before, other.Before...)
	h.After = append(h.After, other.After...)
}

// TableHooks for all operation before and after for the table
type TableHooks struct {
	Insert  TableOperateHook
	Inserts TableOperateHook
	Save    TableOperateHook
	Update  TableOperateHook
	Delete  TableOperateHook
}

// Merge with other hooks
func (h *TableHooks) Merge(other *TableHooks) {
	if other == nil {
		return
	}

	h.Insert.Merge(&other.Insert)
	h.Inserts.Merge(&other.Insert)
	h.Save.Merge(&other.Insert)
	h.Update.Merge(&other.Insert)
	h.Delete.Merge(&other.Insert)
}
