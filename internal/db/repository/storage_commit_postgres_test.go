//go:build postgres

package repository_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
)

func TestPostgresCommitRequestLedger(t *testing.T) {
	for _, c := range commitLedgerCases {
		t.Run(c.name, func(t *testing.T) { c.run(t, newCommitFixture(t, migratedPostgresDB(t))) })
	}
}

// queueTransferredCopy does what a transfer's settlement does with a copy that
// joins no signed request: join a collecting request, or start one.
func queueTransferredCopy(ctx context.Context, f commitFixture, copyRow *model.StorageCopy, newRequestID string) error {
	return f.repos.WithTx(ctx, func(tx *repository.Repositories) error {
		return joinOrStartCommitRequest(ctx, tx, f, copyRow.ID, newRequestID)
	})
}

func joinOrStartCommitRequest(ctx context.Context, tx *repository.Repositories, f commitFixture, copyID int64, newRequestID string) error {
	_, _, err := tx.Contents.JoinCollectingCommitRequest(ctx, repository.JoinCommitRequestInput{
		CopyID: copyID, StorageDataSetID: f.dataSetID, MaxPieces: 32,
	})
	if !errors.Is(err, repository.ErrNotFound) {
		return err
	}
	task, _, err := tx.Tasks.Enqueue(ctx, commitTask(newRequestID))
	if err != nil {
		return err
	}
	return tx.Contents.CreateCollectingCommitRequest(ctx, repository.CreateCommitRequestInput{
		RequestID: newRequestID, TaskID: task.ID, StorageDataSetID: f.dataSetID, CopyIDs: []int64{copyID},
	})
}

// Two transfers of one data set finish together. Neither finds a collecting
// request when it starts looking, but they must not start one each: copies
// transferred together are registered together.
func TestPostgresCopiesTransferredTogetherShareOneRequest(t *testing.T) {
	db := postgresRaceDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	first, second := f.transferredCopy(t, "first"), f.transferredCopy(t, "second")

	hold := newPostgresRaceHold("first", func(query string) bool {
		return strings.Contains(query, "update storage_data_sets") && strings.Contains(query, "updated_at = updated_at")
	})
	db.AddQueryHook(hold)
	defer hold.Release()

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- queueTransferredCopy(postgresRaceContext(ctx, "first"), f, first, "request-first")
	}()
	waitPostgresSignal(t, hold.reached, "first transfer holds the data set")
	secondDone := make(chan error, 1)
	go func() { secondDone <- queueTransferredCopy(ctx, f, second, "request-second") }()
	waitForPostgresLockWait(t, db, "storage_data_sets")
	hold.Release()

	if err := waitPostgresResult(t, firstDone, "first transfer"); err != nil {
		t.Fatalf("queue first: %v", err)
	}
	if err := waitPostgresResult(t, secondDone, "second transfer"); err != nil {
		t.Fatalf("queue second: %v", err)
	}
	for _, copyRow := range []*model.StorageCopy{first, second} {
		if joined := f.copy(t, copyRow.ID); joined.CommitRequestID == nil || *joined.CommitRequestID != "request-first" {
			t.Fatalf("copy %d request = %v, want request-first", copyRow.ID, joined.CommitRequestID)
		}
	}
	if count, err := db.NewSelect().Model((*storagecommit.Request)(nil)).Where("storage_data_set_id = ?", f.dataSetID).Count(ctx); err != nil || count != 1 {
		t.Fatalf("requests = %d, %v, want one", count, err)
	}
}

// A copy that finishes while its data set's collecting request is being
// signed waits for the seal, which holds the data set. It is not in the
// signed set, so it starts the next request instead of being lost or signed
// for.
func TestPostgresCopyArrivingDuringASealStartsTheNextRequest(t *testing.T) {
	db := postgresRaceDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	signed, late := f.transferredCopy(t, "signed"), f.transferredCopy(t, "late")
	taskID := f.collecting(t, "batch", signed)

	hold := newPostgresRaceHold("seal", func(query string) bool {
		return strings.Contains(query, "update storage_commit_requests") && strings.Contains(query, "updated_at = updated_at")
	})
	db.AddQueryHook(hold)
	defer hold.Release()

	sealed := make(chan error, 1)
	go func() {
		_, err := f.repos.Contents.SealCommitRequest(postgresRaceContext(ctx, "seal"), repository.SealCommitRequestInput{
			RequestID: "batch", TaskID: taskID, ExtraDataHex: "abcd",
			Members: []repository.SealMember{{CopyID: signed.ID, ContentID: signed.ContentID, PieceCID: "piece-signed"}},
		})
		sealed <- err
	}()
	waitPostgresSignal(t, hold.reached, "seal holds the request")
	queued := make(chan error, 1)
	go func() { queued <- queueTransferredCopy(ctx, f, late, "request-next") }()
	waitForPostgresLockWait(t, db, "storage_data_sets")
	hold.Release()

	if err := waitPostgresResult(t, sealed, "seal"); err != nil {
		t.Fatalf("SealCommitRequest: %v", err)
	}
	if err := waitPostgresResult(t, queued, "late transfer"); err != nil {
		t.Fatalf("queue late copy: %v", err)
	}
	if request := f.request(t, "batch"); request.Status != storagecommit.RequestStatusReady || request.PieceCount != 1 {
		t.Fatalf("sealed request = %#v, want the signed copy only", request)
	}
	if joined := f.copy(t, late.ID); joined.CommitRequestID == nil || *joined.CommitRequestID != "request-next" || joined.CommitPosition != nil {
		t.Fatalf("late copy = %#v, want it collecting in the next request", joined)
	}
}

// A seal queues the copies that joined after signing in its own transaction,
// while another transfer of the data set looks for a collecting request. Both
// take the data set before the request, so neither waits on the other in a
// cycle and both settle.
func TestPostgresSealThatQueuesLateCopiesDoesNotDeadlockAJoin(t *testing.T) {
	db := postgresRaceDB(t)
	f := newCommitFixture(t, db)
	ctx := t.Context()
	signed, late, arriving := f.transferredCopy(t, "signed"), f.transferredCopy(t, "late"), f.transferredCopy(t, "arriving")
	taskID := f.collecting(t, "batch", signed, late)

	hold := newPostgresRaceHold("seal", func(query string) bool {
		return strings.Contains(query, "update storage_commit_requests") && strings.Contains(query, "updated_at = updated_at")
	})
	db.AddQueryHook(hold)
	defer hold.Release()

	sealed := make(chan error, 1)
	go func() {
		sealCtx := postgresRaceContext(ctx, "seal")
		sealed <- f.repos.WithTx(sealCtx, func(tx *repository.Repositories) error {
			spilled, err := tx.Contents.SealCommitRequest(sealCtx, repository.SealCommitRequestInput{
				RequestID: "batch", TaskID: taskID, ExtraDataHex: "abcd",
				Members: []repository.SealMember{{CopyID: signed.ID, ContentID: signed.ContentID, PieceCID: "piece-signed"}},
			})
			if err != nil {
				return err
			}
			for _, copyID := range spilled {
				if err := joinOrStartCommitRequest(sealCtx, tx, f, copyID, "request-next"); err != nil {
					return err
				}
			}
			return nil
		})
	}()
	waitPostgresSignal(t, hold.reached, "seal holds the request")
	joined := make(chan error, 1)
	go func() { joined <- queueTransferredCopy(ctx, f, arriving, "request-other") }()
	waitForPostgresLockWait(t, db, "storage_")
	hold.Release()

	if err := waitPostgresResult(t, sealed, "seal"); err != nil {
		t.Fatalf("seal and queue late copies: %v", err)
	}
	if err := waitPostgresResult(t, joined, "arriving transfer"); err != nil {
		t.Fatalf("queue arriving copy: %v", err)
	}
	if request := f.request(t, "batch"); request.Status != storagecommit.RequestStatusReady || request.PieceCount != 1 {
		t.Fatalf("sealed request = %#v, want the signed copy only", request)
	}
	for _, copyRow := range []*model.StorageCopy{late, arriving} {
		if next := f.copy(t, copyRow.ID); next.CommitRequestID == nil || *next.CommitRequestID != "request-next" {
			t.Fatalf("copy %d request = %v, want both waiting in the next request", copyRow.ID, next.CommitRequestID)
		}
	}
}
