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

// TableHooks for all operation before and after for the table
type TableHooks struct {
	Insert  TableOperateHook
	Inserts TableOperateHook
	Save    TableOperateHook
	Update  TableOperateHook
	Delete  TableOperateHook
}

// Merge with other hooks
func (h *TableHooks) Merge(newHooks *TableHooks) {
	if newHooks == nil {
		return
	}

	if len(newHooks.Insert.Before) > 0 {
		h.Insert.Before = append(h.Insert.Before, newHooks.Insert.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Insert.After = append(h.Insert.After, newHooks.Insert.After...)
	}
	if len(newHooks.Inserts.Before) > 0 {
		h.Inserts.Before = append(h.Inserts.Before, newHooks.Inserts.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Inserts.After = append(h.Inserts.After, newHooks.Inserts.After...)
	}
	if len(newHooks.Update.Before) > 0 {
		h.Update.Before = append(h.Update.Before, newHooks.Update.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Update.After = append(h.Update.After, newHooks.Update.After...)
	}
	if len(newHooks.Save.Before) > 0 {
		h.Save.Before = append(h.Save.Before, newHooks.Save.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Save.After = append(h.Save.After, newHooks.Save.After...)
	}
	if len(newHooks.Delete.Before) > 0 {
		h.Delete.Before = append(h.Delete.Before, newHooks.Delete.Before...)
	}
	if len(newHooks.Insert.After) > 0 {
		h.Delete.After = append(h.Delete.After, newHooks.Delete.After...)
	}
}
