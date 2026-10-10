package repository

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestCaseSensitivePrefixUpperBound(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
		wantOK bool
	}{
		{name: "ascii", prefix: "abc", want: "abd", wantOK: true},
		{name: "surrogate gap", prefix: string(rune(0xD7FF)), want: string(rune(0xE000)), wantOK: true},
		{name: "trailing max rune", prefix: "a" + string(utf8.MaxRune), want: "b", wantOK: true},
		{name: "all max rune", prefix: string(utf8.MaxRune), wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := stringPrefixUpperBound(tt.prefix)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("stringPrefixUpperBound(%q) = %q, %v; want %q, %v", tt.prefix, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestCaseSensitivePrefixSQLFallsBackWhenUpperBoundUnavailable(t *testing.T) {
	prefix := string(utf8.MaxRune)
	condition, args := caseSensitivePrefixSQL("object_version.key", prefix)
	wantCondition := `object_version.key COLLATE "C" >= ? AND substr(object_version.key, 1, ?) = ?`
	wantArgs := []interface{}{prefix, 1, prefix}
	if condition != wantCondition || !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("caseSensitivePrefixSQL fallback = %q, %#v; want %q, %#v", condition, args, wantCondition, wantArgs)
	}
}

func TestCaseSensitivePrefixUsesCollatedRangeOnPostgres(t *testing.T) {
	condition, args := caseSensitivePrefixSQL("object_version.key", `logs\_%/`)
	wantCondition := `object_version.key COLLATE "C" >= ? AND object_version.key COLLATE "C" < ?`
	wantArgs := []interface{}{`logs\_%/`, `logs\_%0`}
	if condition != wantCondition || !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("caseSensitivePrefixSQL pg = %q, %#v; want %q, %#v", condition, args, wantCondition, wantArgs)
	}
}

func TestBucketStorageHealthMarkerUsesCollatedComparisonsOnPostgres(t *testing.T) {
	filters, args := bucketStorageHealthAffectedVersionObjectFilters(BucketStorageHealthAffectedVersionsInput{
		KeyMarker:       "marker",
		VersionIDMarker: "version",
		CreatedAtMarker: time.Unix(1, 0),
	})
	for _, comparison := range []string{
		`object_version.key COLLATE "C" > ?`,
		`object_version.key COLLATE "C" = ?`,
	} {
		if !strings.Contains(filters, comparison) {
			t.Fatalf("filters = %q, want %q", filters, comparison)
		}
	}
	wantArgs := []interface{}{"marker", "marker", time.Unix(1, 0), time.Unix(1, 0), "version"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
}
