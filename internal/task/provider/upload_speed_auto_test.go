package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
	sdktypes "github.com/strahe/synapse-go/types"
)

type unusedSpeedProbe struct{}

func (unusedSpeedProbe) Probe(context.Context, string) (providerbenchmark.Measurement, error) {
	return providerbenchmark.Measurement{}, nil
}

func newSpeedScheduleHandler(t *testing.T, repos *repository.Repositories, service *observability.Service) *ObservabilityHandler {
	t.Helper()
	registry := taskengine.NewRegistry()
	speed, err := NewUploadSpeedTestHandler(UploadSpeedTestDependencies{Observability: service, UploadSpeedProbe: unusedSpeedProbe{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(speed); err != nil {
		t.Fatal(err)
	}
	tasks, err := taskengine.NewService(registry, repos)
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := tasks.Scheduler("provider.observability", model.TaskTypeProviderUploadSpeedTest)
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewObservabilityHandler(ObservabilityDependencies{Repositories: repos, Observability: service, UploadSpeedProbe: unusedSpeedProbe{}, Scheduler: scheduler})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func speedScheduleState(id uint64, at time.Time) observability.ProviderState {
	providerID := types.NewOnChainID(id)
	url := fmt.Sprintf("https://provider-%d.example", id)
	return observability.ProviderState{
		ProviderID: providerID, Status: observability.StatusAvailable,
		Active: new(true), HasPDP: new(true), ServiceURL: &url, LastCheckedAt: at,
		ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
		Profile: &observability.ProviderProfile{ProviderID: providerID, Active: true, ServiceURL: url, RegistrySnapshot: json.RawMessage(`{}`)},
	}
}

func enqueueScheduledSpeed(t *testing.T, h *ObservabilityHandler, providerID, hash string) {
	t.Helper()
	if err := h.deps.Repositories.WithTx(t.Context(), func(tx *repository.Repositories) error {
		task, _, err := h.deps.Scheduler.EnqueueInTransaction(t.Context(), tx, taskengine.EnqueueRequest{
			Type: model.TaskTypeProviderUploadSpeedTest, Input: providerbenchmark.Input{ProviderID: providerID, ServiceURLHash: hash},
		})
		if err != nil {
			return err
		}
		return tx.ProviderUploadSpeed.Begin(t.Context(), providerID, hash, task.ID)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestProviderSpeedSchedulingUsesCurrentResultsAndScope(t *testing.T) {
	for _, tc := range []struct {
		name                                                                           string
		state                                                                          providerbenchmark.State
		failure                                                                        string
		changedURL, periodic, expired, other, bound, all, busy, unhealthy, wantStarted bool
	}{
		{name: "trusted first measurement", wantStarted: true},
		{name: "bound first measurement", other: true, bound: true, wantStarted: true},
		{name: "unlisted default scope", other: true},
		{name: "new URL", state: providerbenchmark.StateSucceeded, changedURL: true, wantStarted: true},
		{name: "successful result", state: providerbenchmark.StateSucceeded},
		{name: "upload failure", state: providerbenchmark.StateFailed, failure: "upload_failed"},
		{name: "timeout", state: providerbenchmark.StateFailed, failure: "timeout"},
		{name: "non measurement failure", state: providerbenchmark.StateFailed, failure: "interrupted", wantStarted: true},
		{name: "periodic success", state: providerbenchmark.StateSucceeded, periodic: true, expired: true, wantStarted: true},
		{name: "recent success", state: providerbenchmark.StateSucceeded, periodic: true},
		{name: "periodic failure", state: providerbenchmark.StateFailed, failure: "timeout", periodic: true, expired: true, wantStarted: true},
		{name: "active measurement", state: providerbenchmark.StateTesting},
		{name: "unhealthy provider", unhealthy: true},
		{name: "all scope idle", other: true, all: true, wantStarted: true},
		{name: "all scope busy", other: true, all: true, busy: true},
		{name: "all periodic measurement", state: providerbenchmark.StateSucceeded, periodic: true, expired: true, other: true, all: true, wantStarted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testutil.NewTestDB(t)
			repos := repository.NewRepositories(db)
			now := time.Now().UTC()
			state := speedScheduleState(101, now)
			if tc.unhealthy {
				state.Status = observability.StatusUnavailable
			}
			if err := repos.Observability.ReplaceProviderStates(t.Context(), now, []observability.ProviderState{state}); err != nil {
				t.Fatal(err)
			}
			if !tc.other {
				if _, err := repos.Observability.RecordApprovedProviders(t.Context(), now, []types.OnChainID{state.ProviderID}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.bound {
				bucket := &model.Bucket{Name: "speed-bound", DefaultCopies: 1, MinimumDurableCopies: 1}
				if err := repos.Buckets.Create(t.Context(), bucket); err != nil {
					t.Fatal(err)
				}
				if _, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: state.ProviderID}); err != nil {
					t.Fatal(err)
				}
			}
			h := newSpeedScheduleHandler(t, repos, observability.NewService(observability.ServiceOptions{Store: repos.Observability}))
			if tc.all {
				h.deps.UploadSpeedTestAutoScope = "all"
			}
			if tc.periodic {
				h.deps.UploadSpeedTestRetestInterval = time.Hour
			}
			hash := providerbenchmark.URLHash(*state.ServiceURL)
			if tc.changedURL {
				hash = providerbenchmark.URLHash("https://previous.example")
			}
			if tc.state == providerbenchmark.StateTesting {
				enqueueScheduledSpeed(t, h, "101", hash)
			} else if tc.state != "" {
				at := now
				if tc.expired {
					at = now.Add(-2 * time.Hour)
				}
				row := &providerbenchmark.Result{ProviderID: "101", State: tc.state, ServiceURLHash: hash, SampleBytes: providerbenchmark.SampleBytes, TestedAt: &at, CreatedAt: now, UpdatedAt: now}
				if tc.state == providerbenchmark.StateSucceeded {
					row.DurationMS, row.BytesPerSecond = new(int64(1000)), new(providerbenchmark.SampleBytes)
				} else {
					row.FailureCode = &tc.failure
				}
				if _, err := db.NewInsert().Model(row).Exec(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if tc.busy {
				enqueueScheduledSpeed(t, h, "999", providerbenchmark.URLHash("https://busy.example"))
			}
			before, err := db.NewSelect().Model((*model.Task)(nil)).Count(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := h.scheduleMissingProviderSpeedTests(t.Context()); err != nil {
				t.Fatal(err)
			}
			after, err := db.NewSelect().Model((*model.Task)(nil)).Count(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			wantCount := before
			if tc.wantStarted {
				wantCount++
			}
			if after != wantCount {
				t.Fatalf("task count=%d, want %d", after, wantCount)
			}
			if err := h.scheduleMissingProviderSpeedTests(t.Context()); err != nil {
				t.Fatal(err)
			}
			if repeated, err := db.NewSelect().Model((*model.Task)(nil)).Count(t.Context()); err != nil || repeated != after {
				t.Fatalf("repeated scheduling count=%d, want %d, err=%v", repeated, after, err)
			}
		})
	}
}

func TestProviderSpeedSchedulingPrioritizesTrustedAndBoundsOtherQueue(t *testing.T) {
	for _, tc := range []struct {
		name         string
		all, trusted bool
		want         []string
	}{
		{"trusted priority", false, true, []string{"102", "101", "103"}},
		{"all waits for trusted queue", true, true, []string{"102", "101", "103"}},
		{"all idle batch", true, false, []string{"101", "102", "103", "104"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repos := repository.NewRepositories(testutil.NewTestDB(t))
			now := time.Now().UTC()
			states := make([]observability.ProviderState, 7)
			for i := range states {
				states[i] = speedScheduleState(uint64(101+i), now)
			}
			if err := repos.Observability.ReplaceProviderStates(t.Context(), now, states); err != nil {
				t.Fatal(err)
			}
			if tc.trusted {
				if _, err := repos.Observability.RecordApprovedProviders(t.Context(), now, []types.OnChainID{states[0].ProviderID, states[1].ProviderID}); err != nil {
					t.Fatal(err)
				}
				if _, err := repos.Observability.RecordEndorsedProviders(t.Context(), now, []types.OnChainID{states[1].ProviderID}); err != nil {
					t.Fatal(err)
				}
				bucket := &model.Bucket{Name: "speed-priority", DefaultCopies: 1, MinimumDurableCopies: 1}
				if err := repos.Buckets.Create(t.Context(), bucket); err != nil {
					t.Fatal(err)
				}
				if _, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: states[2].ProviderID}); err != nil {
					t.Fatal(err)
				}
			}
			h := newSpeedScheduleHandler(t, repos, observability.NewService(observability.ServiceOptions{Store: repos.Observability}))
			if tc.all {
				h.deps.UploadSpeedTestAutoScope = "all"
			}
			if err := h.scheduleMissingProviderSpeedTests(t.Context()); err != nil {
				t.Fatal(err)
			}
			page, err := repos.Tasks.List(t.Context(), repository.TaskListFilter{Type: model.TaskTypeProviderUploadSpeedTest, Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, task := range slices.Backward(page.Tasks) {
				got = append(got, task.SubjectKey)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("queued provider order=%v, want %v", got, tc.want)
			}
		})
	}
}

type speedApprovedSource struct {
	calls int
	err   error
}

func (s *speedApprovedSource) ReadApprovedProviderIDs(context.Context) ([]types.OnChainID, error) {
	s.calls++
	return []types.OnChainID{types.NewOnChainID(101)}, s.err
}

type speedEndorsedSource struct {
	calls int
	err   error
}

func (s *speedEndorsedSource) GetEndorsedProviderIDs(context.Context) ([]sdktypes.BigInt, error) {
	s.calls++
	return []sdktypes.BigInt{types.NewOnChainID(101).SDK()}, s.err
}

func TestProviderSpeedSchedulingRefreshesMissingConfiguredTier(t *testing.T) {
	approvedErr := errors.New("approved source unavailable")
	endorsedErr := errors.New("endorsed source unavailable")
	for _, tc := range []struct {
		name                           string
		approvedErr, endorsedErr       error
		wantErr                        error
		storedApproved, storedEndorsed bool
		approvedCalls, endorsedCalls   int
	}{
		{name: "missing tiers", approvedCalls: 1, endorsedCalls: 1},
		{name: "approved failure with stored endorsed", approvedErr: approvedErr, wantErr: approvedErr, storedEndorsed: true, approvedCalls: 2},
		{name: "approved failure before endorsed refresh", approvedErr: approvedErr, wantErr: approvedErr, approvedCalls: 2, endorsedCalls: 1},
		{name: "endorsed failure after approved refresh", endorsedErr: endorsedErr, wantErr: endorsedErr, approvedCalls: 1, endorsedCalls: 2},
		{name: "stored tiers with unavailable sources", approvedErr: approvedErr, endorsedErr: endorsedErr, storedApproved: true, storedEndorsed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repos := repository.NewRepositories(testutil.NewTestDB(t))
			now := time.Now().UTC()
			if err := repos.Observability.ReplaceProviderStates(t.Context(), now, []observability.ProviderState{speedScheduleState(101, now)}); err != nil {
				t.Fatal(err)
			}
			if tc.storedApproved {
				if _, err := repos.Observability.RecordApprovedProviders(t.Context(), now, []types.OnChainID{types.NewOnChainID(101)}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.storedEndorsed {
				if _, err := repos.Observability.RecordEndorsedProviders(t.Context(), now, []types.OnChainID{types.NewOnChainID(101)}); err != nil {
					t.Fatal(err)
				}
			}
			approved := &speedApprovedSource{err: tc.approvedErr}
			endorsed := &speedEndorsedSource{err: tc.endorsedErr}
			h := newSpeedScheduleHandler(t, repos, observability.NewService(observability.ServiceOptions{
				Store: repos.Observability, ApprovedProviders: approved, EndorsedProviders: endorsed,
			}))
			for range 2 {
				err := h.scheduleMissingProviderSpeedTests(t.Context())
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("scheduling error=%v, want %v", err, tc.wantErr)
				}
			}
			if approved.calls != tc.approvedCalls || endorsed.calls != tc.endorsedCalls {
				t.Fatalf("source reads approved=%d endorsed=%d, want %d and %d", approved.calls, endorsed.calls, tc.approvedCalls, tc.endorsedCalls)
			}
			if row, err := repos.ProviderUploadSpeed.Get(t.Context(), "101"); err != nil || row == nil || row.State != providerbenchmark.StateTesting {
				t.Fatalf("provider measurement=%#v, err=%v", row, err)
			}
		})
	}
}
