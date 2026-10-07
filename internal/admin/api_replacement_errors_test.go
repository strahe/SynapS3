package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
)

type failedReplacementEligibility struct {
	repository.StorageReplacementRepository
	err error
}

func (r failedReplacementEligibility) SourceEligibility(context.Context, int64) (*model.StorageDataSet, bool, error) {
	return nil, false, r.err
}

func TestBucketReplacementEligibilityLogsUnexpectedFailures(t *testing.T) {
	for _, cause := range []error{errors.New("database unavailable"), storagereplacement.ErrActiveReplacement, &storagereplacement.SourceOutcomeError{Message: "Check storage setup."}} {
		t.Run(cause.Error(), func(t *testing.T) {
			f := newReplacementAPIFixture(t, nil)
			var logs bytes.Buffer
			f.srv.logger = slog.New(slog.NewJSONHandler(&logs, nil))
			f.srv.repos.Replacements = failedReplacementEligibility{StorageReplacementRepository: f.srv.repos.Replacements, err: cause}
			response := httptest.NewRecorder()
			f.mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/buckets/"+f.bucket.Name, nil))
			var detail bucketDetailResponse
			if response.Code != http.StatusOK {
				t.Fatalf("detail: %d %s", response.Code, response.Body.String())
			}
			if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil {
				t.Fatal(err)
			}
			if len(detail.DataSets) != 1 || detail.DataSets[0].Replaceable {
				t.Fatalf("eligibility did not fail closed: %#v", detail.DataSets)
			}
			unexpected := storagereplacement.Code(cause) == ""
			if got := strings.Contains(logs.String(), "failed to check data set replacement eligibility"); got != unexpected {
				t.Fatalf("unexpected diagnostic: %s", logs.String())
			}
			if unexpected && (strings.Contains(response.Body.String(), cause.Error()) || !strings.Contains(logs.String(), cause.Error())) {
				t.Fatal("database failure must be logged without being exposed to the client")
			}
		})
	}
}
