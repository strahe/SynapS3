package model

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	_ "modernc.org/sqlite"
)

func TestStorageOnChainIDColumnsUseTextInPostgresDDL(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file::memory:")
	if err != nil {
		t.Fatalf("open sqlite handle: %v", err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })

	db := bun.NewDB(sqldb, pgdialect.New())
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name    string
		model   interface{}
		columns []string
	}{
		{
			name:    "storage_data_sets",
			model:   (*StorageDataSet)(nil),
			columns: []string{"provider_id", "data_set_id", "client_data_set_id"},
		},
		{
			name:    "storage_upload_copies",
			model:   (*StorageUploadCopy)(nil),
			columns: []string{"provider_id", "piece_id"},
		},
		{
			name:    "storage_upload_failures",
			model:   (*StorageUploadFailure)(nil),
			columns: []string{"provider_id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddl := strings.ToLower(db.NewCreateTable().Model(tt.model).String())
			for _, column := range tt.columns {
				if !strings.Contains(ddl, `"`+column+`" text`) {
					t.Fatalf("DDL for %s column %s = %s, want text type", tt.name, column, ddl)
				}
				if strings.Contains(ddl, `"`+column+`" jsonb`) {
					t.Fatalf("DDL for %s column %s = %s, want non-jsonb type", tt.name, column, ddl)
				}
			}
		})
	}
}
