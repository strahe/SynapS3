package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type actorIdentityResolverFunc func(context.Context, common.Address) (string, string, error)

func (f actorIdentityResolverFunc) ResolveActorID(ctx context.Context, address common.Address) (string, string, error) {
	return f(ctx, address)
}

func TestProviderIdentityResolverEnrichesSavedProfileOnly(t *testing.T) {
	address := "0x1111111111111111111111111111111111111111"
	called := make(chan struct{}, 1)
	resolver := newProviderIdentityResolver(actorIdentityResolverFunc(func(_ context.Context, got common.Address) (string, string, error) {
		if got != common.HexToAddress(address) {
			t.Errorf("actor address = %s, want %s", got.Hex(), address)
		}
		called <- struct{}{}
		return "f410fabc", "f01234", nil
	}), time.Minute, time.Now, testLogger())
	events := make(chan string, 1)
	resolver.SetProviderIdentityPublisher(func(id string) { events <- id })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	saved := map[string]*providerIdentityResponse{"101": {RegistryProviderID: "101", Name: "saved", ServiceProviderAddress: address}}
	first := resolver.EnrichActors(saved)["101"]
	if first.Name != "saved" || first.FilecoinActorID != "" {
		t.Fatalf("initial identity = %#v", first)
	}
	waitForActorSignal(t, called)
	if id := waitForActorSignal(t, events); id != "101" {
		t.Fatalf("event ID = %q, want 101", id)
	}
	saved["101"].Name = "new saved name"
	enriched := resolver.EnrichActors(saved)["101"]
	if enriched.Name != "new saved name" || enriched.FilecoinAddress != "f410fabc" || enriched.FilecoinActorID != "f01234" {
		t.Fatalf("enriched identity = %#v, want current saved name and cached actor", enriched)
	}
}

func TestProviderIdentityResolverDiscardsResultForChangedAddress(t *testing.T) {
	firstAddress := common.HexToAddress("0x1111111111111111111111111111111111111111")
	secondAddress := common.HexToAddress("0x2222222222222222222222222222222222222222")
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var firstOnce, secondOnce sync.Once
	resolver := newProviderIdentityResolver(actorIdentityResolverFunc(func(ctx context.Context, address common.Address) (string, string, error) {
		switch address {
		case firstAddress:
			firstOnce.Do(func() { close(firstStarted) })
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return "", "", ctx.Err()
			}
			return "f410fold", "f0101", nil
		case secondAddress:
			secondOnce.Do(func() { close(secondStarted) })
			select {
			case <-releaseSecond:
			case <-ctx.Done():
				return "", "", ctx.Err()
			}
			return "f410fnew", "f0202", nil
		default:
			return "", "", errors.New("unexpected address")
		}
	}), time.Minute, time.Now, testLogger())
	events := make(chan string, 2)
	resolver.SetProviderIdentityPublisher(func(id string) { events <- id })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)

	saved := map[string]*providerIdentityResponse{"101": {RegistryProviderID: "101", ServiceProviderAddress: firstAddress.Hex()}}
	resolver.EnrichActors(saved)
	waitForActorSignal(t, firstStarted)
	saved["101"].ServiceProviderAddress = secondAddress.Hex()
	resolver.EnrichActors(saved)
	waitForActorSignal(t, secondStarted)
	close(releaseFirst)
	if got := resolver.EnrichActors(saved)["101"]; got.FilecoinActorID != "" || got.FilecoinAddress != "" {
		t.Fatalf("identity after old lookup = %#v, want no old actor fields", got)
	}
	close(releaseSecond)
	if id := waitForActorSignal(t, events); id != "101" {
		t.Fatalf("event ID = %q, want 101", id)
	}
	got := resolver.EnrichActors(saved)["101"]
	if got.FilecoinAddress != "f410fnew" || got.FilecoinActorID != "f0202" {
		t.Fatalf("identity after new lookup = %#v", got)
	}
	select {
	case id := <-events:
		t.Fatalf("extra event for stale address: %s", id)
	default:
	}
}

func TestProviderIdentityResolverKeepsPartialFilecoinAddress(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	resolver := newProviderIdentityResolver(actorIdentityResolverFunc(func(context.Context, common.Address) (string, string, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return "f410fabc", "", errors.New("lookup failed")
	}), time.Minute, time.Now, testLogger())
	events := make(chan string, 1)
	resolver.SetProviderIdentityPublisher(func(id string) { events <- id })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go resolver.Run(ctx)
	saved := map[string]*providerIdentityResponse{"101": {RegistryProviderID: "101", ServiceProviderAddress: "0x1111111111111111111111111111111111111111"}}
	resolver.EnrichActors(saved)
	waitForActorSignal(t, events)
	got := resolver.EnrichActors(saved)["101"]
	if got.FilecoinAddress != "f410fabc" || got.FilecoinActorID != "" {
		t.Fatalf("partial actor identity = %#v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("actor calls = %d, want backoff after failure", calls)
	}
}

func waitForActorSignal[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(time.Second):
		t.Fatal("actor lookup did not complete")
		var zero T
		return zero
	}
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
