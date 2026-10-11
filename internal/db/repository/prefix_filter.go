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

func caseSensitivePrefixSQL(column string, prefix string) (string, []interface{}) {
	if prefix == "" {
		return "", nil
	}
	keyExpr := keyOrderExpr(column)
	if upper, ok := stringPrefixUpperBound(prefix); ok {
		return fmt.Sprintf("%s >= ? AND %s < ?", keyExpr, keyExpr), []interface{}{prefix, upper}
	}
	return fmt.Sprintf("%s >= ? AND substr(%s, 1, ?) = ?", keyExpr, column), []interface{}{prefix, utf8.RuneCountInString(prefix), prefix}
}

func keyOrderExpr(column string) string {
	return fmt.Sprintf(`%s COLLATE "C"`, column)
}

func keyComparisonSQL(column string, operator string) string {
	return fmt.Sprintf("%s %s ?", keyOrderExpr(column), operator)
}

func stringPrefixUpperBound(prefix string) (string, bool) {
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
