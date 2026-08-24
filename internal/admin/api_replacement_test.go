package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	idtypes "github.com/strahe/synaps3/internal/types"
)

type stubProviderSelector struct {
	providers []string
	err       error
	calls     int
}

func onChainIDValue(value string) idtypes.OnChainID {
	id, err := idtypes.ParseOnChainID("test id", value)
	if err != nil {
		panic(err)
	}
	return id
}

func (s *stubProviderSelector) ListReplacementProviders(context.Context) ([]idtypes.OnChainID, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	providers := make([]idtypes.OnChainID, 0, len(s.providers))
	for _, id := range s.providers {
		providers = append(providers, onChainIDValue(id))
	}
	return providers, nil
}

type replacementAPIFixture struct {
	srv     *Server
	mux     *http.ServeMux
	bucket  *model.Bucket
	source  *model.StorageDataSet
	request int
}

func newReplacementAPIFixture(t *testing.T, selector providerReplacementSelector) *replacementAPIFixture {
	t.Helper()
	srv, _ := newBucketAPITestServer(t)
	if selector != nil {
		srv.WithProviderReplacement(selector)
	}
	ctx := context.Background()
	bucket := &model.Bucket{Name: "replacement-bucket", Status: model.BucketStatusActive}
	if err := srv.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}
	binding, err := srv.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:   bucket.ID,
		ProviderID: onChainIDValue("101"),
		CopyIndex:  0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := srv.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		DataSetID: onChainIDValue("1001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	source, err := srv.repos.Uploads.GetDataSetBindingByID(ctx, binding.ID)
	if err != nil || source == nil {
		t.Fatalf("GetDataSetBindingByID: %#v err=%v", source, err)
	}
	return &replacementAPIFixture{srv: srv, mux: newBucketAPIMux(srv), bucket: bucket, source: source}
}

func (f *replacementAPIFixture) start(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	f.request++
	if !strings.Contains(body, `"client_request_id"`) && strings.HasSuffix(body, "}") {
		body = strings.TrimSuffix(body, "}")
		if body != "{" {
			body += ","
		}
		body += `"client_request_id":"test-request-` + strconv.Itoa(f.request) + `"}`
	}
	return f.startRaw(t, body)
}

func (f *replacementAPIFixture) startRaw(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	path := "/api/v1/buckets/" + f.bucket.Name + "/data-sets/" +
		strconv.FormatInt(f.source.ID, 10) + "/replacement"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	return rec
}

func decodeReplacement(t *testing.T, rec *httptest.ResponseRecorder) providerReplacementResponse {
	t.Helper()
	var body providerReplacementResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode replacement response: %v", err)
	}
	return body
}

func decodeAPIError(t *testing.T, rec *httptest.ResponseRecorder) map[string]string {
	t.Helper()
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return body
}

func TestAPIStartDataSetReplacementManualMode(t *testing.T) {
	fixture := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})

	rec := fixture.start(t, `{"mode":"manual","provider_id":"202"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s, want 201", rec.Code, rec.Body.String())
	}
	body := decodeReplacement(t, rec)
	if body.Status != string(storagereplacement.StatusPreparingTarget) {
		t.Fatalf("status = %s, want preparing_target", body.Status)
	}
	if body.Source.ProviderID != "101" || body.Target.ProviderID != "202" {
		t.Fatalf("providers = %s -> %s, want 101 -> 202", body.Source.ProviderID, body.Target.ProviderID)
	}
	// Preparing the target must not move writes.
	if !body.Source.IsCurrent || body.Target.IsCurrent {
		t.Fatalf("currency = source:%v target:%v, want the source still current", body.Source.IsCurrent, body.Target.IsCurrent)
	}
	if body.ItemsTotal != 0 || body.ItemsCopied != 0 {
		t.Fatalf("progress = %d/%d, want no migration work yet", body.ItemsCopied, body.ItemsTotal)
	}
	if body.Progress == nil || body.Progress.Scope != "provider_replacement" || body.Progress.SeedingComplete || body.Progress.Percent != nil {
		t.Fatalf("structured progress = %#v, want indeterminate provider replacement progress", body.Progress)
	}
}

// Automatic selection must exclude every provider the bucket has ever used.
func TestAPIStartDataSetReplacementAutomaticMode(t *testing.T) {
	// 101 already serves this bucket, so automatic selection has to walk past it.
	selector := &stubProviderSelector{providers: []string{"101", "303"}}
	fixture := newReplacementAPIFixture(t, selector)

	rec := fixture.start(t, `{"mode":"automatic"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s, want 201", rec.Code, rec.Body.String())
	}
	body := decodeReplacement(t, rec)
	if body.Target.ProviderID != "303" || body.SelectionMode != string(storagereplacement.SelectionModeAutomatic) {
		t.Fatalf("body = %#v, want the automatically selected provider", body)
	}
}

func TestAPIStartDataSetReplacementIsIdempotent(t *testing.T) {
	selector := &stubProviderSelector{providers: []string{"202", "303"}}
	fixture := newReplacementAPIFixture(t, selector)
	body := `{"mode":"manual","provider_id":"202","client_request_id":"same-confirmation"}`

	first := fixture.startRaw(t, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d body=%s, want 201", first.Code, first.Body.String())
	}
	firstReplacement := decodeReplacement(t, first)
	ctx := context.Background()
	if err := fixture.srv.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID: firstReplacement.Target.ID, DataSetID: onChainIDValue("2002"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := fixture.srv.repos.Replacements.Activate(ctx, firstReplacement.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	replay := fixture.startRaw(t, body)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d body=%s, want 200", replay.Code, replay.Body.String())
	}
	if got := decodeReplacement(t, replay); got.ID != firstReplacement.ID {
		t.Fatalf("replay replacement = %d, want original %d", got.ID, firstReplacement.ID)
	}
	if selector.calls != 1 {
		t.Fatalf("provider inventory calls = %d, want replay before inventory lookup", selector.calls)
	}

	conflict := fixture.startRaw(t, `{"mode":"manual","provider_id":"303","client_request_id":"same-confirmation"}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body=%s, want 409", conflict.Code, conflict.Body.String())
	}
	if got := decodeAPIError(t, conflict)["code"]; got != storagereplacement.CodeIdempotencyConflict {
		t.Fatalf("conflict code = %q, want %q", got, storagereplacement.CodeIdempotencyConflict)
	}
}

func TestAPIStartDataSetReplacementAutomaticReplayIgnoresRegistryDrift(t *testing.T) {
	selector := &stubProviderSelector{providers: []string{"202"}}
	fixture := newReplacementAPIFixture(t, selector)
	body := `{"mode":"automatic","client_request_id":"automatic-replay"}`
	first := fixture.startRaw(t, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d body=%s, want 201", first.Code, first.Body.String())
	}
	original := decodeReplacement(t, first)
	selector.providers = []string{"303"}
	replay := fixture.startRaw(t, body)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d body=%s, want 200", replay.Code, replay.Body.String())
	}
	got := decodeReplacement(t, replay)
	if got.ID != original.ID || got.Target.ProviderID != original.Target.ProviderID {
		t.Fatalf("replay = replacement %d provider %s, want %d/%s", got.ID, got.Target.ProviderID, original.ID, original.Target.ProviderID)
	}
	if selector.calls != 1 {
		t.Fatalf("provider inventory calls = %d, want the replay to avoid registry drift", selector.calls)
	}
}

func TestAPIStartDataSetReplacementRejections(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		selector providerReplacementSelector
		status   int
		code     string
	}{
		{name: "unknown mode", body: `{"mode":"guess"}`, status: http.StatusBadRequest, code: storagereplacement.CodeTargetInvalid},
		{name: "manual without provider", body: `{"mode":"manual"}`, status: http.StatusBadRequest, code: storagereplacement.CodeTargetInvalid},
		{name: "manual naming the source", body: `{"mode":"manual","provider_id":"101"}`, status: http.StatusBadRequest, code: storagereplacement.CodeTargetInvalid},
		{name: "manual provider unavailable", body: `{"mode":"manual","provider_id":"202"}`, selector: &stubProviderSelector{providers: []string{"303"}}, status: http.StatusBadRequest, code: storagereplacement.CodeTargetUnavailable},
		{name: "automatic with a provider", body: `{"mode":"automatic","provider_id":"202"}`, status: http.StatusBadRequest, code: storagereplacement.CodeTargetInvalid},
		{name: "unknown field", body: `{"mode":"manual","provider":"202"}`, status: http.StatusBadRequest},
		{name: "automatic without a storage service", body: `{"mode":"automatic"}`, status: http.StatusServiceUnavailable},
		{
			name:     "no eligible provider",
			body:     `{"mode":"automatic"}`,
			selector: &stubProviderSelector{providers: []string{"101"}},
			status:   http.StatusConflict,
			code:     storagereplacement.CodeNoEligibleProvider,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newReplacementAPIFixture(t, tc.selector)
			rec := fixture.start(t, tc.body)
			if rec.Code != tc.status {
				t.Fatalf("status = %d body=%s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if tc.code != "" {
				if got := decodeAPIError(t, rec)["code"]; got != tc.code {
					t.Fatalf("code = %q, want %q", got, tc.code)
				}
			}
		})
	}
}

func TestAPIStartDataSetReplacementRequiresClientRequestID(t *testing.T) {
	fixture := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
	rec := fixture.startRaw(t, `{"mode":"manual","provider_id":"202"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s, want 400", rec.Code, rec.Body.String())
	}
}

func TestAPIStartDataSetReplacementRejectsSecondConfirmation(t *testing.T) {
	fixture := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202", "303"}})
	if rec := fixture.start(t, `{"mode":"manual","provider_id":"202"}`); rec.Code != http.StatusCreated {
		t.Fatalf("first confirmation status = %d", rec.Code)
	}
	// A second confirmation supersedes the first rather than colliding, which is
	// how an operator changes their mind about the target.
	rec := fixture.start(t, `{"mode":"manual","provider_id":"303"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("second confirmation status = %d body=%s, want 201", rec.Code, rec.Body.String())
	}
}

func TestAPIRetryStorageReplacementStates(t *testing.T) {
	fixture := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
	ctx := context.Background()
	rec := fixture.start(t, `{"mode":"manual","provider_id":"202"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("confirmation status = %d", rec.Code)
	}
	created := decodeReplacement(t, rec)

	retry := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/storage-replacements/"+strconv.FormatInt(created.ID, 10)+"/retry", nil)
		out := httptest.NewRecorder()
		fixture.mux.ServeHTTP(out, req)
		return out
	}

	// Work that is still progressing is not the operator's to retry.
	out := retry()
	if out.Code != http.StatusConflict {
		t.Fatalf("retry while preparing = %d, want 409", out.Code)
	}
	if got := decodeAPIError(t, out)["code"]; got != storagereplacement.CodeNotRetryable {
		t.Fatalf("code = %q, want %q", got, storagereplacement.CodeNotRetryable)
	}

	if err := fixture.srv.repos.Replacements.MarkFailed(ctx, created.ID, nil, "target creation exhausted"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	out = retry()
	if out.Code != http.StatusOK {
		t.Fatalf("retry after failure = %d body=%s, want 200", out.Code, out.Body.String())
	}
	if body := decodeReplacement(t, out); body.LastError != nil {
		t.Fatalf("last_error = %v, want it cleared by the retry", *body.LastError)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/storage-replacements/999999/retry", nil)
	out = httptest.NewRecorder()
	fixture.mux.ServeHTTP(out, req)
	if out.Code != http.StatusNotFound {
		t.Fatalf("retry unknown replacement = %d, want 404", out.Code)
	}
}

func TestAPIRetryStorageReplacementRejectsPermanentTargetConflict(t *testing.T) {
	fixture := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
	body := `{"mode":"manual","provider_id":"202","client_request_id":"permanent-failure"}`
	rec := fixture.startRaw(t, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("confirmation status = %d body=%s", rec.Code, rec.Body.String())
	}
	created := decodeReplacement(t, rec)
	reason := storagereplacement.FailureReasonTargetInUse
	if err := fixture.srv.repos.Replacements.MarkFailed(context.Background(), created.ID, &reason, "target already serves the bucket"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	replay := fixture.startRaw(t, body)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d body=%s, want 200", replay.Code, replay.Body.String())
	}
	if got := decodeReplacement(t, replay).FailureReason; got != string(reason) {
		t.Fatalf("failure_reason = %q, want %q", got, reason)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/storage-replacements/"+strconv.FormatInt(created.ID, 10)+"/retry", nil)
	retry := httptest.NewRecorder()
	fixture.mux.ServeHTTP(retry, req)
	if retry.Code != http.StatusConflict {
		t.Fatalf("retry status = %d body=%s, want 409", retry.Code, retry.Body.String())
	}
	if got := decodeAPIError(t, retry)["code"]; got != storagereplacement.CodeTargetInUse {
		t.Fatalf("retry code = %q, want %q", got, storagereplacement.CodeTargetInUse)
	}
}

// Replacement work carries state the task queue knows nothing about, so the
// generic retry must refuse it and point the operator at the Data Sets surface.
func TestRetryExhaustedRejectsReplacementCoordinator(t *testing.T) {
	srv, _ := newBucketAPITestServer(t)
	ctx := context.Background()
	stage := storagereplacement.StageMigrate
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "bucket",
		RefID:          1,
		IdempotencyKey: storagereplacement.MigrateTaskKey(42),
		Payload:        storagereplacement.NewMigratePayload(42),
		Status:         model.TaskStatusQueued,
		MaxRetries:     1,
	}
	if err := srv.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}
	if _, err := srv.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusExhausted).
		Where("id = ?", task.ID).
		Exec(ctx); err != nil {
		t.Fatalf("mark exhausted: %v", err)
	}

	err := srv.repos.Tasks.RetryExhausted(ctx, task.ID)
	if !errors.Is(err, repository.ErrReplacementRetryUnsupported) {
		t.Fatalf("RetryExhausted = %v, want the replacement rejection", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/tasks/{id}/retry", srv.handleRetryExhausted)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/"+strconv.FormatInt(task.ID, 10)+"/retry", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d body=%s, want 409", rec.Code, rec.Body.String())
	}
	body := decodeAPIError(t, rec)
	if body["code"] != storagereplacement.CodeTaskRetryUnsupported {
		t.Fatalf("code = %q, want %q", body["code"], storagereplacement.CodeTaskRetryUnsupported)
	}
	if !strings.Contains(body["error"], "Data Sets") {
		t.Fatalf("error = %q, want it to point at the Data Sets surface", body["error"])
	}
}

func (f *replacementAPIFixture) listProviders(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	path := "/api/v1/buckets/" + f.bucket.Name + "/data-sets/" +
		strconv.FormatInt(f.source.ID, 10) + "/replacement/providers"
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// The chooser reports the same eligibility the confirmation enforces, and keeps
// ineligible providers visible with the reason.
func TestAPIListDataSetReplacementProviders(t *testing.T) {
	fixture := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"101", "303"}})
	rec := fixture.listProviders(t)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
	var body struct {
		Providers []replacementProviderResponse `json:"providers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode providers: %v", err)
	}
	if len(body.Providers) != 2 {
		t.Fatalf("providers = %#v, want every approved provider listed", body.Providers)
	}
	source, replacement := body.Providers[0], body.Providers[1]
	if source.ProviderID != "101" || source.Eligible ||
		source.IneligibleReason != providerIneligibleCurrentSource {
		t.Fatalf("source provider = %#v, want it listed but not choosable", source)
	}
	if replacement.ProviderID != "303" || !replacement.Eligible || replacement.IneligibleReason != "" {
		t.Fatalf("replacement provider = %#v, want it choosable", replacement)
	}
}

func TestAPIListDataSetReplacementProvidersRejections(t *testing.T) {
	t.Run("without a storage service", func(t *testing.T) {
		fixture := newReplacementAPIFixture(t, nil)
		if rec := fixture.listProviders(t); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d body=%s, want 503", rec.Code, rec.Body.String())
		}
	})
	t.Run("unknown data set", func(t *testing.T) {
		fixture := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"303"}})
		fixture.source.ID = 987654
		if rec := fixture.listProviders(t); rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d body=%s, want 404", rec.Code, rec.Body.String())
		}
	})
}
