//go:build postgres

package repository_test

import "testing"

func TestPostgresSubjectViews(t *testing.T) { testSubjectViews(t, migratedPostgresDB(t)) }
