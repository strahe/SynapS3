package provider

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/testutil"
	idtypes "github.com/strahe/synaps3/internal/types"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type catalogEventRecorder struct{ published bool }

func (r *catalogEventRecorder) Publish(topic string, _ map[string]any) {
	if topic == "provider_catalog_updated" {
		r.published = true
	}
}

type partialCatalogChecker struct {
	events         *catalogEventRecorder
	dataSetAttempt bool
}

func (c *partialCatalogChecker) CheckProviders(_ context.Context, checkedAt time.Time, _ []observability.LocalDataSet) ([]observability.ProviderState, error) {
	return []observability.ProviderState{speedScheduleState(101, checkedAt)}, nil
}

func (c *partialCatalogChecker) CheckDataSets(context.Context, time.Time, []observability.LocalDataSet) ([]observability.DataSetState, error) {
	c.dataSetAttempt = true
	if !c.events.published {
		return nil, errors.New("provider catalog event was not published before data set refresh")
	}
	return nil, errors.New("data set refresh failed")
}

func TestObservabilityRefreshPublishesCommittedProvidersBeforeDataSetFailure(t *testing.T) {
	repos := repository.NewRepositories(testutil.NewTestDB(t))
	events := &catalogEventRecorder{}
	checker := &partialCatalogChecker{events: events}
	service := observability.NewService(observability.ServiceOptions{
		Store: repos.Observability, Checker: checker,
		LocalDataSets: observability.LocalDataSetSourceFunc(func(context.Context) ([]observability.LocalDataSet, error) { return nil, nil }),
	})
	if _, err := repos.Observability.RecordApprovedProviders(t.Context(), time.Now().UTC(), []idtypes.OnChainID{idtypes.NewOnChainID(101)}); err != nil {
		t.Fatal(err)
	}
	handler := newSpeedScheduleHandler(t, repos, service)
	handler.deps.Events = events
	handler.Execute(t.Context(), taskengine.Execution{})
	if !checker.dataSetAttempt || !events.published {
		t.Fatalf("data set attempted=%t, provider event published=%t", checker.dataSetAttempt, events.published)
	}
	page, err := repos.Observability.ListProviderStates(context.Background(), observability.ListOptions{})
	if err != nil || len(page.Items) != 1 || page.Items[0].ProviderID.String() != "101" {
		t.Fatalf("committed providers = %#v, error=%v", page.Items, err)
	}
	if row, err := repos.ProviderUploadSpeed.Get(t.Context(), "101"); err != nil || row == nil || row.State != providerbenchmark.StateTesting {
		t.Fatalf("measurement after data set refresh failure=%#v, err=%v", row, err)
	}
}
