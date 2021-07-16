package sqlm

import (
	"fmt"
	"strings"
)

// Query sql select query info.
type Query struct {
	Distinct      bool
	Columns       []string
	From          string
	Where         string
	OrderByColumn string
	OrderDesc     bool
}

// String implement interface fmt.Stringer.
func (s *Query) String() string {
	var distinct string
	if s.Distinct {
		distinct = "distinct"
	}
	query := fmt.Sprintf("select %s %s from %s", distinct, strings.Join(s.Columns, ","), s.From)
	if s.Where != "" {
		query += " where " + s.Where
	}
	if s.OrderByColumn != "" {
		query += " ORDER BY " + s.OrderByColumn
		if s.OrderDesc {
			query += " DESC"
		}
	}
	return query
}
