package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/spregistry"
	sdktypes "github.com/strahe/synapse-go/types"
)

type fakeProviderIdentityRegistry struct {
	mu sync.Mutex

	pdp  map[string]spregistry.PDPProvider
	info map[string]*spregistry.ProviderInfo

	pdpBatchCalls  int
	infoBatchCalls int
	pdpBatchIDs    [][]string
	infoBatchIDs   [][]string

	pdpErr       error
	infoErr      error
	blockPDP     chan struct{}
	pdpStarted   chan struct{}
	pdpStartedDo sync.Once
}

func (f *fakeProviderIdentityRegistry) GetPDPProvidersByIDs(ctx context.Context, providerIDs []sdktypes.BigInt) ([]spregistry.PDPProvider, error) {
	if f.pdpStarted != nil {
		f.pdpStartedDo.Do(func() { close(f.pdpStarted) })
	}
	if f.blockPDP != nil {
		select {
		case <-f.blockPDP:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	ids := sdkIDStrings(providerIDs)
	f.mu.Lock()
	f.pdpBatchCalls++
	f.pdpBatchIDs = append(f.pdpBatchIDs, ids)
	err := f.pdpErr
	out := make([]spregistry.PDPProvider, 0, len(providerIDs))
	for _, id := range ids {
		if p, ok := f.pdp[id]; ok {
			out = append(out, p)
		}
	}
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (f *fakeProviderIdentityRegistry) GetProvidersByIDs(_ context.Context, providerIDs []sdktypes.BigInt) ([]*spregistry.ProviderInfo, error) {
	ids := sdkIDStrings(providerIDs)
	f.mu.Lock()
	defer f.mu.Unlock()

	f.infoBatchCalls++
	f.infoBatchIDs = append(f.infoBatchIDs, ids)
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	out := make([]*spregistry.ProviderInfo, len(providerIDs))
	for i, id := range ids {
		out[i] = f.info[id]
	}
	return out, nil
}

func (f *fakeProviderIdentityRegistry) batchCounts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pdpBatchCalls, f.infoBatchCalls
}

func (f *fakeProviderIdentityRegistry) batchIDs() ([][]string, [][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.pdpBatchIDs...), append([][]string(nil), f.infoBatchIDs...)
}

type fakeActorIdentityResolver struct {
	filecoinAddress string
	actorID         string
	err             error
	block           chan struct{}

	mu    sync.Mutex
	calls int
}

func (f *fakeActorIdentityResolver) ResolveActorID(ctx context.Context, _ common.Address) (string, string, error) {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return "", "", ctx.Err()
		}
	}
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		return f.filecoinAddress, f.actorID, f.err
	}
	return f.filecoinAddress, f.actorID, nil
}

func (f *fakeActorIdentityResolver) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestProviderIdentityResolverReturnsCachedSnapshotAndRefreshesInBackground(t *testing.T) {
	registry := &fakeProviderIdentityRegistry{
		pdp: map[string]spregistry.PDPProvider{
			"101": testPDPProvider("101", "alpha-pdp", "https://alpha.example"),
		},
		blockPDP:   make(chan struct{}),
		pdpStarted: make(chan struct{}),
	}
	actors := &fakeActorIdentityResolver{filecoinAddress: "f410fabc", actorID: "f01234"}
	resolver := newProviderIdentityResolver(registry, actors, time.Minute, time.Now, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	got := resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	if len(got) != 0 {
		t.Fatalf("ProviderIdentities first snapshot = %#v, want cache miss without blocking", got)
	}
	select {
	case <-registry.pdpStarted:
	case <-time.After(time.Second):
		t.Fatal("background provider refresh did not start")
	}

	close(registry.blockPDP)
	identity := waitForProviderIdentity(t, resolver, "101", func(identity *providerIdentityResponse) bool {
		return identity.Name == "alpha-pdp" && identity.FilecoinActorID == "f01234"
	})
	if identity.ServiceURL != "https://alpha.example" || identity.RegistryProviderID != "101" {
		t.Fatalf("identity = %#v, want refreshed PDP provider identity", identity)
	}

	got = resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	if got["101"] == nil || got["101"].Name != identity.Name || got["101"].FilecoinActorID != identity.FilecoinActorID {
		t.Fatalf("cached snapshot = %#v, want cached identity fields", got)
	}
	pdpCalls, _ := registry.batchCounts()
	if pdpCalls != 1 || actors.callCount() != 1 {
		t.Fatalf("calls = registry:%d actors:%d, want cache hit after background refresh", pdpCalls, actors.callCount())
	}
}

func TestProviderIdentityResolverUsesBatchPDPProviderLookupAndProviderInfoFallback(t *testing.T) {
	registry := &fakeProviderIdentityRegistry{
		pdp: map[string]spregistry.PDPProvider{
			"101": testPDPProvider("101", "alpha-pdp", "https://alpha.example"),
		},
		info: map[string]*spregistry.ProviderInfo{
			"202": {
				ID:              sdktypes.NewBigInt(202),
				ServiceProvider: common.HexToAddress("0x2222222222222222222222222222222222222222"),
				Name:            "metadata-only-provider",
				IsActive:        false,
			},
		},
	}
	resolver := newProviderIdentityResolver(registry, nil, time.Minute, time.Now, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101"), onChainID(t, "202")})
	waitForProviderIdentity(t, resolver, "101", func(identity *providerIdentityResponse) bool {
		return identity.Name == "alpha-pdp" && identity.ServiceURL == "https://alpha.example"
	})
	waitForProviderIdentity(t, resolver, "202", func(identity *providerIdentityResponse) bool {
		return identity.Name == "metadata-only-provider" && identity.ServiceURL == ""
	})

	pdpIDs, infoIDs := registry.batchIDs()
	if !reflect.DeepEqual(pdpIDs, [][]string{{"101", "202"}}) {
		t.Fatalf("PDP batch IDs = %#v, want one batch with both IDs", pdpIDs)
	}
	if !reflect.DeepEqual(infoIDs, [][]string{{"202"}}) {
		t.Fatalf("provider info fallback IDs = %#v, want only missing PDP ID", infoIDs)
	}
}

func TestProviderIdentityResolverPublishesRegistryIdentityBeforeActorLookup(t *testing.T) {
	registry := &fakeProviderIdentityRegistry{
		pdp: map[string]spregistry.PDPProvider{
			"101": testPDPProvider("101", "alpha-pdp", "https://alpha.example"),
		},
	}
	actors := &fakeActorIdentityResolver{
		filecoinAddress: "f410fabc",
		actorID:         "f01234",
		block:           make(chan struct{}),
	}
	resolver := newProviderIdentityResolver(registry, actors, time.Minute, time.Now, testLogger())

	events := make(chan *providerIdentityResponse, 2)
	resolver.SetProviderIdentityPublisher(func(identity *providerIdentityResponse) {
		events <- identity
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	first := waitForProviderIdentityEvent(t, events)
	if first.Name != "alpha-pdp" || first.FilecoinActorID != "" {
		t.Fatalf("first event = %#v, want registry identity before actor fields", first)
	}

	close(actors.block)
	second := waitForProviderIdentityEvent(t, events)
	if second.Name != "alpha-pdp" || second.FilecoinActorID != "f01234" || second.FilecoinAddress != "f410fabc" {
		t.Fatalf("second event = %#v, want actor-enriched identity", second)
	}
}

func TestProviderIdentityResolverPreservesActorFieldsDuringRegistryRefresh(t *testing.T) {
	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	registry := &fakeProviderIdentityRegistry{
		pdp: map[string]spregistry.PDPProvider{
			"101": testPDPProvider("101", "alpha-pdp", "https://alpha.example"),
		},
	}
	actors := &fakeActorIdentityResolver{filecoinAddress: "f410fabc", actorID: "f01234"}
	resolver := newProviderIdentityResolver(registry, actors, time.Minute, func() time.Time { return now }, testLogger())

	events := make(chan *providerIdentityResponse, 4)
	resolver.SetProviderIdentityPublisher(func(identity *providerIdentityResponse) {
		events <- identity
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	waitForProviderIdentity(t, resolver, "101", func(identity *providerIdentityResponse) bool {
		return identity.Name == "alpha-pdp" && identity.FilecoinActorID == "f01234"
	})
	if actors.callCount() != 1 {
		t.Fatalf("actor calls = %d, want initial actor lookup only", actors.callCount())
	}

	registry.mu.Lock()
	registry.pdp["101"] = testPDPProvider("101", "alpha-renamed", "https://alpha.example")
	registry.mu.Unlock()
	now = now.Add(time.Minute + time.Second)

	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	refreshed := waitForProviderIdentity(t, resolver, "101", func(identity *providerIdentityResponse) bool {
		return identity.Name == "alpha-renamed"
	})
	if refreshed.FilecoinAddress != "f410fabc" || refreshed.FilecoinActorID != "f01234" {
		t.Fatalf("refreshed identity = %#v, want registry refresh to preserve actor fields", refreshed)
	}
	refreshedEvent := waitForProviderIdentityEventMatch(t, events, func(identity *providerIdentityResponse) bool {
		return identity.Name == "alpha-renamed"
	})
	if refreshedEvent.FilecoinAddress != "f410fabc" || refreshedEvent.FilecoinActorID != "f01234" {
		t.Fatalf("refreshed event = %#v, want published identity to preserve actor fields", refreshedEvent)
	}
	time.Sleep(20 * time.Millisecond)
	if actors.callCount() != 1 {
		t.Fatalf("actor calls = %d, want registry refresh with cached actor fields not to re-resolve actor", actors.callCount())
	}
}

func TestProviderIdentityResolverStoresFilecoinAddressWhenActorIDLookupFails(t *testing.T) {
	registry := &fakeProviderIdentityRegistry{
		pdp: map[string]spregistry.PDPProvider{
			"101": testPDPProvider("101", "alpha-pdp", "https://alpha.example"),
		},
	}
	actors := &fakeActorIdentityResolver{filecoinAddress: "f410fabc", err: errors.New("lookup id failed")}
	resolver := newProviderIdentityResolver(registry, actors, time.Minute, time.Now, testLogger())

	events := make(chan *providerIdentityResponse, 4)
	resolver.SetProviderIdentityPublisher(func(identity *providerIdentityResponse) {
		events <- identity
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	partial := waitForProviderIdentity(t, resolver, "101", func(identity *providerIdentityResponse) bool {
		return identity.Name == "alpha-pdp" && identity.FilecoinAddress == "f410fabc"
	})
	if partial.FilecoinActorID != "" {
		t.Fatalf("partial identity = %#v, want Filecoin address without actor ID", partial)
	}
	partialEvent := waitForProviderIdentityEventMatch(t, events, func(identity *providerIdentityResponse) bool {
		return identity.FilecoinAddress == "f410fabc"
	})
	if partialEvent.FilecoinActorID != "" {
		t.Fatalf("partial event = %#v, want Filecoin address without actor ID", partialEvent)
	}
	if actors.callCount() != 1 {
		t.Fatalf("actor calls = %d, want one failed actor lookup", actors.callCount())
	}
}

func TestProviderIdentityResolverBacksOffFailedRefresh(t *testing.T) {
	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	registry := &fakeProviderIdentityRegistry{pdpErr: errors.New("registry unavailable")}
	resolver := newProviderIdentityResolver(registry, nil, time.Minute, func() time.Time { return now }, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	waitForProviderRefreshCalls(t, registry, 1)
	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	time.Sleep(20 * time.Millisecond)
	pdpCalls, _ := registry.batchCounts()
	if pdpCalls != 1 {
		t.Fatalf("PDP batch calls = %d, want failed provider refresh to back off", pdpCalls)
	}
}

func TestProviderIdentityResolverRetriesActorLookupAfterBackoffForFreshCache(t *testing.T) {
	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	registry := &fakeProviderIdentityRegistry{
		pdp: map[string]spregistry.PDPProvider{
			"101": testPDPProvider("101", "alpha-pdp", "https://alpha.example"),
		},
	}
	actors := &fakeActorIdentityResolver{err: errors.New("rpc unavailable")}
	resolver := newProviderIdentityResolver(registry, actors, time.Hour, func() time.Time { return now }, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	waitForProviderIdentity(t, resolver, "101", func(identity *providerIdentityResponse) bool {
		return identity.Name == "alpha-pdp"
	})
	waitForActorCalls(t, actors, 1)

	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	time.Sleep(20 * time.Millisecond)
	if actors.callCount() != 1 {
		t.Fatalf("actor calls = %d, want actor lookup to back off", actors.callCount())
	}

	now = now.Add(defaultProviderIdentityRefreshBackoff + time.Second)
	resolver.ProviderIdentities([]idtypes.OnChainID{onChainID(t, "101")})
	waitForActorCalls(t, actors, 2)
}

func TestLotusActorIdentityResolverIncludesHTTPErrorBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"code":401,"message":"bad token"}}`, http.StatusUnauthorized)
	}))
	defer ts.Close()

	resolver := &lotusActorIdentityResolver{rpcURL: ts.URL, httpClient: ts.Client()}
	err := resolver.call(context.Background(), "Filecoin.StateLookupID", nil, new(string))
	if err == nil {
		t.Fatal("call error = nil, want HTTP error")
	}
	if got := err.Error(); !strings.Contains(got, "http 401") || !strings.Contains(got, "bad token") {
		t.Fatalf("call error = %q, want HTTP status and response body", got)
	}
}

func TestLotusActorIdentityResolverLimitsResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxFilecoinRPCResponseBodyBytes+1)))
	}))
	defer ts.Close()

	resolver := &lotusActorIdentityResolver{rpcURL: ts.URL, httpClient: ts.Client()}
	err := resolver.call(context.Background(), "Filecoin.StateLookupID", nil, new(string))
	if err == nil {
		t.Fatal("call error = nil, want response size error")
	}
	if got := err.Error(); !strings.Contains(got, "response body exceeds") {
		t.Fatalf("call error = %q, want response body size error", got)
	}
}

func testPDPProvider(id string, name string, serviceURL string) spregistry.PDPProvider {
	providerID, err := sdktypes.ParseBigInt(id)
	if err != nil {
		panic("invalid test provider ID")
	}
	return spregistry.PDPProvider{
		Info: spregistry.ProviderInfo{
			ID:              providerID,
			ServiceProvider: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Payee:           common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Name:            name,
			Description:     "test provider",
			IsActive:        true,
		},
		Offering: spregistry.PDPOffering{
			ServiceURL: serviceURL,
			Location:   "C=US;ST=California;L=San Francisco",
			ExtraCapabilities: map[string][]byte{
				"serviceStatus": []byte("prod"),
				"capacityTib":   []byte("12"),
			},
		},
	}
}

func sdkIDStrings(ids []sdktypes.BigInt) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return out
}

func waitForProviderIdentity(t *testing.T, resolver *ProviderIdentityResolver, providerID string, accept func(*providerIdentityResponse) bool) *providerIdentityResponse {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	id := onChainID(t, providerID)
	for time.Now().Before(deadline) {
		identity := resolver.ProviderIdentities([]idtypes.OnChainID{id})[providerID]
		if identity != nil && accept(identity) {
			return identity
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("provider identity %s did not match condition", providerID)
	return nil
}

func waitForProviderIdentityEvent(t *testing.T, events <-chan *providerIdentityResponse) *providerIdentityResponse {
	t.Helper()
	select {
	case identity := <-events:
		return identity
	case <-time.After(time.Second):
		t.Fatal("provider identity event not published")
		return nil
	}
}

func waitForProviderIdentityEventMatch(t *testing.T, events <-chan *providerIdentityResponse, accept func(*providerIdentityResponse) bool) *providerIdentityResponse {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case identity := <-events:
			if accept(identity) {
				return identity
			}
		case <-deadline:
			t.Fatal("provider identity event did not match condition")
			return nil
		}
	}
}

func waitForProviderRefreshCalls(t *testing.T, registry *fakeProviderIdentityRegistry, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		pdpCalls, _ := registry.batchCounts()
		if pdpCalls >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	pdpCalls, _ := registry.batchCounts()
	t.Fatalf("PDP batch calls = %d, want at least %d", pdpCalls, want)
}

func waitForActorCalls(t *testing.T, actors *fakeActorIdentityResolver, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if actors.callCount() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("actor calls = %d, want at least %d", actors.callCount(), want)
}
