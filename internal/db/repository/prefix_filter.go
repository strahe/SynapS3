package repository

import (
	"fmt"
	"strings"
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
	// SynapS3 currently stores S3 keys as UTF-8 strings; byte-exact non-UTF-8 key semantics
	// require a separate storage and API contract.
	if dialectName == dialect.PG {
		// PostgreSQL LIKE is case-sensitive; index strategy is handled separately from this semantic helper.
		return fmt.Sprintf("%s LIKE ? ESCAPE '\\'", column), []interface{}{escapeSQLLikePattern(prefix) + "%"}
	}
	// SQLite LIKE is case-insensitive by default, so use a range/fallback predicate instead.
	if upper, ok := stringPrefixUpperBound(prefix); ok {
		return fmt.Sprintf("%s >= ? AND %s < ?", column, column), []interface{}{prefix, upper}
	}
	return fmt.Sprintf("%s >= ? AND substr(%s, 1, ?) = ?", column, column), []interface{}{prefix, utf8.RuneCountInString(prefix), prefix}
}

func escapeSQLLikePattern(value string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(value)
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
