package repository_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func TestSubjectViews(t *testing.T) { testSubjectViews(t, testDB(t)) }

func testSubjectViews(t *testing.T, db *bun.DB) {
	f := newCommitFixture(t, db)
	ctx := t.Context()
	first := f.transferredCopy(t, "older/current/file.txt")
	second := f.transferredCopy(t, "second.txt")
	row, err := f.repos.Contents.GetCopySubject(ctx, first.ID)
	if err != nil || row.ContentID != first.ContentID || *row.Size != 10 || *row.CopyIndex != 0 || row.ProviderID.String() != "701" {
		t.Fatalf("copy subject=%#v err=%v", row, err)
	}
	row, err = f.repos.Contents.GetDataSetSubject(ctx, f.dataSetID)
	if err != nil || *row.LocalDataSetID != f.dataSetID || row.DataSetID.String() != "801" {
		t.Fatalf("dataset subject=%#v err=%v", row, err)
	}
	row, err = f.repos.Contents.GetContentSubject(ctx, first.ContentID)
	if err != nil || *row.Size != 10 || row.BucketID != f.bucket.ID {
		t.Fatalf("content subject=%#v err=%v", row, err)
	}
	requestID := "registration:subject-test"
	taskID := f.collecting(t, requestID, first, second)
	row, err = f.repos.Contents.GetCommitSubject(ctx, requestID)
	if err != nil || *row.ContentCount != 2 {
		t.Fatalf("collecting subject=%#v err=%v", row, err)
	}
	f.seal(t, requestID, taskID, first)
	if _, err := db.NewDelete().Model((*model.StorageCopy)(nil)).Where("id = ?", first.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	row, err = f.repos.Contents.GetCommitSubject(ctx, requestID)
	if err != nil || *row.ContentCount != 1 {
		t.Fatalf("retained request subject=%#v err=%v", row, err)
	}
	if _, err := f.repos.Contents.GetCopySubject(ctx, first.ID); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("deleted copy err=%v", err)
	}

	newer := newObjectVersion(f.bucket.ID, "newer/history/file.txt", "newer-version", 10)
	newer.ContentID, newer.CreatedAt = &first.ContentID, time.Now().UTC().Add(time.Hour)
	if _, err := createVersion(t, f.repos, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewUpdate().Model((*model.Object)(nil)).Set("current_version_id = NULL").Where("key = ?", newer.Key).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	sample, err := f.repos.Objects.GetContentFileSample(ctx, first.ContentID)
	if err != nil || sample.Key != "older/current/file.txt" || sample.Source != "current" || sample.OtherVersions != 1 {
		t.Fatalf("current sample=%#v err=%v", sample, err)
	}
	if _, err := db.NewUpdate().Model((*model.Object)(nil)).Set("current_version_id = NULL").Where("bucket_id = ?", f.bucket.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	sample, err = f.repos.Objects.GetContentFileSample(ctx, first.ContentID)
	if err != nil || sample.Key != newer.Key || sample.Source != "historical" || sample.OtherVersions != 1 {
		t.Fatalf("historical sample=%#v err=%v", sample, err)
	}
	if _, err := db.NewDelete().Model((*model.ObjectVersion)(nil)).Where("content_id = ?", first.ContentID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	deletedAt := time.Now().UTC()
	for _, key := range []string{"first/deleted.txt", "last/deleted.txt"} {
		deletion := &model.ObjectDeletion{BucketID: f.bucket.ID, ObjectID: 1, VersionID: key, Key: key, ContentID: &first.ContentID, Size: 10, DeletedAt: deletedAt}
		if _, err := db.NewInsert().Model(deletion).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	sample, err = f.repos.Objects.GetContentFileSample(ctx, first.ContentID)
	if err != nil || sample.Key != "last/deleted.txt" || sample.Source != "deleted" || sample.OtherVersions != 1 {
		t.Fatalf("deleted sample=%#v err=%v", sample, err)
	}
	if _, err := f.repos.Objects.GetContentFileSample(ctx, 999999); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("missing sample err=%v", err)
	}
	providerID := onChainID(t, "184467440737095516160")
	profile := &observability.ProviderProfile{ProviderID: providerID, Name: "North", ServiceURL: "https://provider.example", RegistrySnapshot: json.RawMessage(`{}`), LastSuccessAt: deletedAt}
	if _, err := db.NewInsert().Model(profile).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	provider, err := f.repos.Observability.GetProviderSubject(ctx, providerID)
	if err != nil || provider.ProviderID.String() != providerID.String() || provider.Name != "North" || provider.ServiceURL != profile.ServiceURL {
		t.Fatalf("provider subject=%#v err=%v", provider, err)
	}
	op, _, err := f.repos.WalletOperations.CreateOrGet(ctx, repository.CreateWalletOperationInput{Type: model.WalletOperationTypeWithdraw, ClientRequestID: "subject-withdraw", Amount: "123456789012345678901"})
	if err != nil {
		t.Fatal(err)
	}
	wallet, err := f.repos.WalletOperations.GetOperationSubject(ctx, op.ID)
	if err != nil || wallet.Type != op.Type || wallet.Amount != op.Amount {
		t.Fatalf("wallet subject=%#v err=%v", wallet, err)
	}
	target := &model.StorageDataSet{BucketID: f.bucket.ID, ProviderID: onChainID(t, "702"), CopyIndex: 0, Generation: 2, Status: model.StorageDataSetStatusPending}
	if _, err := db.NewInsert().Model(target).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	replacement := &storagereplacement.Replacement{BucketID: f.bucket.ID, CopyIndex: 0, SourceDataSetID: f.dataSetID, TargetDataSetID: target.ID, SelectionMode: storagereplacement.SelectionModeManual, RequestedProviderID: &target.ProviderID, ClientRequestID: "subject-replacement", PriceListFingerprint: "price", Status: storagereplacement.StatusPreparingTarget}
	if _, err := db.NewInsert().Model(replacement).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	replaced, err := f.repos.Replacements.GetReplacementSubject(ctx, replacement.ID)
	if err != nil || replaced.SourceProviderID.String() != "701" || replaced.TargetProviderID.String() != "702" || replaced.CopyIndex != 0 {
		t.Fatalf("replacement subject=%#v err=%v", replaced, err)
	}
}
