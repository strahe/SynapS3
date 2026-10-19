package admin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
)

type failingUploadSpeedHistory struct {
	repository.ProviderUploadSpeedRepository
}

func (failingUploadSpeedHistory) ListLastSuccessByProviderURLs(context.Context, map[string]string) (map[string]providerbenchmark.SuccessfulMeasurement, error) {
	return nil, errors.New("history unavailable")
}

func seedUploadSpeedHistory(t *testing.T, srv *Server, providerID, serviceURL string) time.Time {
	t.Helper()
	input, err := json.Marshal(providerbenchmark.Input{ProviderID: providerID, ServiceURLHash: providerbenchmark.URLHash(serviceURL)})
	if err != nil {
		t.Fatal(err)
	}
	row, _, err := srv.repos.Tasks.Enqueue(t.Context(), &model.Task{
		Type: model.TaskTypeProviderUploadSpeedTest, InputVersion: 1, Input: input, Runtime: json.RawMessage(`{}`),
		SubjectType: "provider", SubjectKey: providerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := json.Marshal(providerbenchmark.Checkpoint{Attempted: true, SampleBytes: providerbenchmark.SampleBytes, DurationMS: 1000, BytesPerSecond: providerbenchmark.SampleBytes})
	if err != nil {
		t.Fatal(err)
	}
	finishedAt := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := srv.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusCompleted).
		Set("finished_at = ?", finishedAt).Set("checkpoint_json = ?::jsonb", string(checkpoint)).Where("id = ?", row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	return finishedAt
}

func TestUploadSpeedViewUsesCurrentProfileURL(t *testing.T) {
	oldURL := "https://old.example"
	durationMS, bytesPerSecond := int64(1000), int64(1024)
	testedAt := time.Date(2026, time.October, 15, 0, 0, 0, 0, time.UTC)
	failureCode := "upload_failed"
	success := providerbenchmark.Result{
		State: providerbenchmark.StateSucceeded, ServiceURLHash: providerbenchmark.URLHash(oldURL),
		SampleBytes: 1024, DurationMS: &durationMS, BytesPerSecond: &bytesPerSecond, TestedAt: &testedAt,
	}
	failed := providerbenchmark.Result{
		State: providerbenchmark.StateFailed, ServiceURLHash: success.ServiceURLHash,
		SampleBytes: 1024, TestedAt: &testedAt, FailureCode: &failureCode,
	}
	testingSuccess, testingFailure := success, failed
	testingSuccess.State = providerbenchmark.StateTesting
	testingFailure.State = providerbenchmark.StateTesting
	for _, tt := range []struct {
		name        string
		row         providerbenchmark.Result
		profile     *observability.ProviderProfile
		observedURL *string
		state       string
		wantSuccess bool
		wantFailure bool
	}{
		{name: "profile URL takes precedence", row: success, profile: &observability.ProviderProfile{ServiceURL: "https://new.example"}, observedURL: &oldURL, state: "stale"},
		{name: "observed URL fallback", row: success, observedURL: &oldURL, state: "succeeded", wantSuccess: true},
		{name: "profile without URL", row: success, profile: &observability.ProviderProfile{}, observedURL: &oldURL, state: "stale"},
		{name: "no current URL", row: success, state: "stale"},
		{name: "retesting retains success", row: testingSuccess, profile: &observability.ProviderProfile{ServiceURL: oldURL}, state: "testing", wantSuccess: true},
		{name: "retesting retains failure", row: testingFailure, profile: &observability.ProviderProfile{ServiceURL: oldURL}, state: "testing", wantFailure: true},
		{name: "retesting old URL", row: testingSuccess, profile: &observability.ProviderProfile{ServiceURL: "https://new.example"}, state: "stale"},
		{name: "testing without result", row: providerbenchmark.Result{State: providerbenchmark.StateTesting, ServiceURLHash: success.ServiceURLHash, SampleBytes: 1024}, observedURL: &oldURL, state: "testing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := uploadSpeedView(tt.row, tt.profile, tt.observedURL)
			if got.State != tt.state {
				t.Fatalf("state = %q, want %q", got.State, tt.state)
			}
			if tt.wantSuccess {
				if got.BytesPerSecond == nil || *got.BytesPerSecond != bytesPerSecond || got.DurationMS == nil || *got.DurationMS != durationMS || got.TestedAt == nil || !got.TestedAt.Equal(testedAt) {
					t.Fatalf("successful result fields missing: %+v", got)
				}
			} else if got.BytesPerSecond != nil || got.DurationMS != nil {
				t.Fatalf("invalid speed result exposed: %+v", got)
			}
			if tt.wantFailure {
				if got.FailureCode == nil || *got.FailureCode != failureCode || got.TestedAt == nil || !got.TestedAt.Equal(testedAt) {
					t.Fatalf("failed result fields missing: %+v", got)
				}
			} else if got.FailureCode != nil {
				t.Fatalf("invalid failure result exposed: %+v", got)
			}
		})
	}
}
