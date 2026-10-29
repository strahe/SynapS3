package repository

import (
	"fmt"
	"unicode/utf8"

	"github.com/uptrace/bun"
)

func applyCaseSensitivePrefixFilter(q *bun.SelectQuery, column string, prefix string) *bun.SelectQuery {
	condition, args := caseSensitivePrefixSQL(column, prefix)
	if condition == "" {
		return q
	}
	return q.Where(condition, args...)
}

func caseSensitivePrefixSQL(column string, prefix string) (string, []any) {
	if prefix == "" {
		return "", nil
	}
	keyExpr := keyOrderExpr(column)
	if upper, ok := PrefixUpperBound(prefix); ok {
		return fmt.Sprintf("%s >= ? AND %s < ?", keyExpr, keyExpr), []any{prefix, upper}
	}
	return fmt.Sprintf("%s >= ? AND substr(%s, 1, ?) = ?", keyExpr, column), []any{prefix, utf8.RuneCountInString(prefix), prefix}
}

func keyOrderExpr(column string) string {
	return fmt.Sprintf(`%s COLLATE "C"`, column)
}

func keyComparisonSQL(column string, operator string) string {
	return fmt.Sprintf("%s %s ?", keyOrderExpr(column), operator)
}

// PrefixUpperBound returns the first key beyond all keys with prefix.
func PrefixUpperBound(prefix string) (string, bool) {
	runes := []rune(prefix)
	for i := len(runes) - 1; i >= 0; i-- {
		if runes[i] == utf8.MaxRune {
			continue
		}
		if runes[i] == 0xD7FF {
			// Skip the UTF-16 surrogate range; valid UTF-8 scalar values resume at U+E000.
			runes[i] = 0xE000
		} else {
			runes[i]++
		}
		return string(runes[:i+1]), true
	}
	return "", false
}
