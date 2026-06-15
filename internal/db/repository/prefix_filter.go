package repository

import (
	"fmt"
	"unicode/utf8"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func applyCaseSensitivePrefixFilter(db bun.IDB, q *bun.SelectQuery, column string, prefix string) *bun.SelectQuery {
	condition, args := caseSensitivePrefixSQL(db, column, prefix)
	if condition == "" {
		return q
	}
	return q.Where(condition, args...)
}

func caseSensitivePrefixSQL(db bun.IDB, column string, prefix string) (string, []interface{}) {
	return caseSensitivePrefixSQLForDialect(db.Dialect().Name(), column, prefix)
}

func caseSensitivePrefixSQLForDialect(dialectName dialect.Name, column string, prefix string) (string, []interface{}) {
	if prefix == "" {
		return "", nil
	}
	keyExpr := keyOrderExprForDialect(dialectName, column)
	if upper, ok := stringPrefixUpperBound(prefix); ok {
		return fmt.Sprintf("%s >= ? AND %s < ?", keyExpr, keyExpr), []interface{}{prefix, upper}
	}
	return fmt.Sprintf("%s >= ? AND substr(%s, 1, ?) = ?", keyExpr, column), []interface{}{prefix, utf8.RuneCountInString(prefix), prefix}
}

func keyOrderExpr(db bun.IDB, column string) string {
	return keyOrderExprForDialect(db.Dialect().Name(), column)
}

func keyOrderExprForDialect(dialectName dialect.Name, column string) string {
	if dialectName == dialect.PG {
		return fmt.Sprintf(`%s COLLATE "C"`, column)
	}
	return column
}

func keyComparisonSQL(db bun.IDB, column string, operator string) string {
	return fmt.Sprintf("%s %s ?", keyOrderExpr(db, column), operator)
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
