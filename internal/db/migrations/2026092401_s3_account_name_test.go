package migrations

import (
	"testing"

	"github.com/uptrace/bun"
)

func TestS3AccountNameMigrationPreservesAccountsAndEnforcesUniqueNames(t *testing.T) {
	testMigrationDialects(t, func(t *testing.T, db *bun.DB) {
		ctx := t.Context()
		if err := runMigrationBody(ctx, db, up2026090101InitialSchema); err != nil {
			t.Fatal(err)
		}
		insert := func(accessKey string, name *string) error {
			if name == nil {
				_, err := db.NewRaw(`INSERT INTO s3_accounts (access_key, secret_key, role, is_root, created_at, updated_at)
					VALUES (?, 'secret', 'user', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, accessKey).Exec(ctx)
				return err
			}
			_, err := db.NewRaw(`INSERT INTO s3_accounts (access_key, name, secret_key, role, is_root, created_at, updated_at)
				VALUES (?, ?, 'secret', 'user', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, accessKey, *name).Exec(ctx)
			return err
		}
		if err := insert("old-1", nil); err != nil {
			t.Fatal(err)
		}
		if err := insert("old-2", nil); err != nil {
			t.Fatal(err)
		}
		if err := runMigrationBody(ctx, db, up2026092401S3AccountName); err != nil {
			t.Fatal(err)
		}
		var name string
		if err := db.NewRaw("SELECT name FROM s3_accounts WHERE access_key = 'old-1'").Scan(ctx, &name); err != nil || name != "" {
			t.Fatalf("old account name = %q, err = %v", name, err)
		}
		if err := insert("new-empty", new(string)); err != nil {
			t.Fatalf("second unnamed account: %v", err)
		}
		alice, lowerAlice := "Alice", "alice"
		if err := insert("alice-1", &alice); err != nil {
			t.Fatal(err)
		}
		if err := insert("alice-2", &lowerAlice); err != nil {
			t.Fatalf("case-distinct name: %v", err)
		}
		if err := insert("duplicate", &alice); err == nil {
			t.Fatal("duplicate nonempty name accepted")
		}
	})
}
