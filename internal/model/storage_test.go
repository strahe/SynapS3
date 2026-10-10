package model

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func TestStorageOnChainIDColumnsUseTextInPostgresDDL(t *testing.T) {
	// Rendering DDL never connects, so the handle needs no server.
	sqldb, err := sql.Open("pgx", "")
	if err != nil {
		t.Fatalf("open PostgreSQL handle: %v", err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })

	db := bun.NewDB(sqldb, pgdialect.New())
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name    string
		model   any
		columns []string
	}{
		{
			name:    "storage_data_sets",
			model:   (*StorageDataSet)(nil),
			columns: []string{"provider_id", "data_set_id", "client_data_set_id"},
		},
		{
			name:    "storage_copies",
			model:   (*StorageCopy)(nil),
			columns: []string{"provider_id", "piece_id"},
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
