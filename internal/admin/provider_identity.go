package admin

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const (
	defaultProviderIdentityTTL            = 5 * time.Minute
	defaultProviderIdentityRPCTimeout     = 5 * time.Second
	defaultProviderIdentityActorTimeout   = 2 * time.Second
	defaultProviderIdentityRefreshBackoff = 60 * time.Second
	defaultProviderIdentityQueueSize      = 128
	defaultProviderIdentityActorWorkers   = 4
	maxFilecoinRPCResponseBodyBytes       = 1 << 20
)

type providerIdentityResponse struct {
	RegistryProviderID     string            `json:"registry_provider_id"`
	Name                   string            `json:"name,omitempty"`
	Description            string            `json:"description,omitempty"`
	ServiceProviderAddress string            `json:"service_provider_address,omitempty"`
	PayeeAddress           string            `json:"payee_address,omitempty"`
	FilecoinAddress        string            `json:"filecoin_address,omitempty"`
	FilecoinActorID        string            `json:"filecoin_actor_id,omitempty"`
	ServiceURL             string            `json:"service_url,omitempty"`
	Location               string            `json:"location,omitempty"`
	ExtraCapabilities      map[string]string `json:"extra_capabilities,omitempty"`
}

type providerIdentityLookup interface {
	EnrichActors(map[string]*providerIdentityResponse) map[string]*providerIdentityResponse
}

type providerIdentityRunner interface {
	Run(context.Context)
}

type providerIdentityPublisherSetter interface {
	SetProviderIdentityPublisher(func(string))
}

type actorIdentityResolver interface {
	ResolveActorID(context.Context, common.Address) (filecoinAddress string, actorID string, err error)
}

type actorCacheKey struct {
	providerID string
	address    string
}

type actorCacheEntry struct {
	filecoinAddress string
	actorID         string
	expiresAt       time.Time
}

// ProviderIdentityResolver caches only Filecoin fields derived from a saved service provider address.
type ProviderIdentityResolver struct {
	actors         actorIdentityResolver
	ttl            time.Duration
	now            func() time.Time
	logger         *slog.Logger
	actorTimeout   time.Duration
	refreshBackoff time.Duration
	actorQueue     chan actorCacheKey

	mu               sync.Mutex
	current          map[string]actorCacheKey
	cache            map[actorCacheKey]actorCacheEntry
	refreshing       map[actorCacheKey]struct{}
	backoffs         map[actorCacheKey]time.Time
	publishIdentity  func(string)
	actorWorkerCount int
}

func NewProviderIdentityResolver(rpcURL string, logger *slog.Logger) *ProviderIdentityResolver {
	if rpcURL == "" {
		return nil
	}
	actors := &lotusActorIdentityResolver{rpcURL: rpcURL, httpClient: &http.Client{Timeout: defaultProviderIdentityRPCTimeout}}
	return newProviderIdentityResolver(actors, defaultProviderIdentityTTL, time.Now, logger)
}

func newProviderIdentityResolver(actors actorIdentityResolver, ttl time.Duration, now func() time.Time, logger *slog.Logger) *ProviderIdentityResolver {
	if now == nil {
		now = time.Now
	}
	return &ProviderIdentityResolver{
		actors:           actors,
		ttl:              ttl,
		now:              now,
		logger:           logger,
		actorTimeout:     defaultProviderIdentityActorTimeout,
		refreshBackoff:   defaultProviderIdentityRefreshBackoff,
		actorQueue:       make(chan actorCacheKey, defaultProviderIdentityQueueSize),
		current:          make(map[string]actorCacheKey),
		cache:            make(map[actorCacheKey]actorCacheEntry),
		refreshing:       make(map[actorCacheKey]struct{}),
		backoffs:         make(map[actorCacheKey]time.Time),
		actorWorkerCount: defaultProviderIdentityActorWorkers,
	}
}

func (r *ProviderIdentityResolver) SetProviderIdentityPublisher(publish func(string)) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.publishIdentity = publish
}

func (r *ProviderIdentityResolver) Run(ctx context.Context) {
	if r == nil {
		return
	}
	var workers sync.WaitGroup
	for i := 0; i < r.actorWorkerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			r.runActorWorker(ctx)
		}()
	}
	workers.Wait()
}

func (r *ProviderIdentityResolver) EnrichActors(identities map[string]*providerIdentityResponse) map[string]*providerIdentityResponse {
	out := make(map[string]*providerIdentityResponse, len(identities))
	if r == nil {
		for id, identity := range identities {
			out[id] = cloneProviderIdentity(identity)
		}
		return out
	}
	for id, identity := range identities {
		if identity == nil {
			continue
		}
		enriched := cloneProviderIdentity(identity)
		key := actorCacheKey{providerID: id}
		if common.IsHexAddress(identity.ServiceProviderAddress) {
			key.address = common.HexToAddress(identity.ServiceProviderAddress).Hex()
		}
		r.mu.Lock()
		if previous, ok := r.current[id]; ok && previous != key {
			delete(r.cache, previous)
			delete(r.backoffs, previous)
		}
		r.current[id] = key
		cached, ok := r.cache[key]
		if ok && key.address != "" {
			enriched.FilecoinAddress = cached.filecoinAddress
			enriched.FilecoinActorID = cached.actorID
		}
		shouldRefresh := key.address != "" && r.actors != nil && (!ok || cached.actorID == "" || (!cached.expiresAt.IsZero() && !r.now().Before(cached.expiresAt)))
		r.mu.Unlock()
		out[id] = enriched
		if shouldRefresh {
			r.enqueueActor(key)
		}
	}
	return out
}

func (r *ProviderIdentityResolver) enqueueActor(key actorCacheKey) {
	r.mu.Lock()
	if r.current[key.providerID] != key {
		r.mu.Unlock()
		return
	}
	if _, active := r.refreshing[key]; active {
		r.mu.Unlock()
		return
	}
	if until := r.backoffs[key]; r.now().Before(until) {
		r.mu.Unlock()
		return
	}
	r.refreshing[key] = struct{}{}
	r.mu.Unlock()
	select {
	case r.actorQueue <- key:
	default:
		r.mu.Lock()
		delete(r.refreshing, key)
		r.mu.Unlock()
		if r.logger != nil {
			r.logger.Debug("provider identity: actor queue full", "provider_id", key.providerID)
		}
	}
}

func (r *ProviderIdentityResolver) runActorWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-r.actorQueue:
			r.resolveActor(ctx, key)
		}
	}
}

func (r *ProviderIdentityResolver) resolveActor(ctx context.Context, key actorCacheKey) {
	defer func() {
		r.mu.Lock()
		delete(r.refreshing, key)
		r.mu.Unlock()
	}()
	r.mu.Lock()
	current := r.current[key.providerID] == key
	r.mu.Unlock()
	if !current {
		return
	}
	callCtx := ctx
	var cancel context.CancelFunc
	if r.actorTimeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, r.actorTimeout)
		defer cancel()
	}
	filecoinAddress, actorID, err := r.actors.ResolveActorID(callCtx, common.HexToAddress(key.address))
	r.mu.Lock()
	if r.current[key.providerID] != key {
		r.mu.Unlock()
		return
	}
	old := r.cache[key]
	next := old
	if err == nil {
		next.filecoinAddress = filecoinAddress
		next.actorID = actorID
	} else if filecoinAddress != "" && old.filecoinAddress == "" {
		next.filecoinAddress = filecoinAddress
	}
	if next != old || err == nil {
		expiresAt := time.Time{}
		if r.ttl > 0 {
			expiresAt = r.now().Add(r.ttl)
		}
		next.expiresAt = expiresAt
		r.cache[key] = next
	}
	if err != nil && r.refreshBackoff > 0 {
		r.backoffs[key] = r.now().Add(r.refreshBackoff)
	} else {
		delete(r.backoffs, key)
	}
	publish := r.publishIdentity
	changed := old.filecoinAddress != next.filecoinAddress || old.actorID != next.actorID
	r.mu.Unlock()
	if err != nil && r.logger != nil {
		r.logger.Debug("provider identity: failed to resolve actor id", "provider_id", key.providerID, "error", err)
	}
	if changed && publish != nil {
		publish(key.providerID)
	}
}

func cloneProviderIdentity(identity *providerIdentityResponse) *providerIdentityResponse {
	if identity == nil {
		return nil
	}
	out := *identity
	if identity.ExtraCapabilities != nil {
		out.ExtraCapabilities = make(map[string]string, len(identity.ExtraCapabilities))
		for key, value := range identity.ExtraCapabilities {
			out.ExtraCapabilities[key] = value
		}
	}
	return &out
}

func identityCapabilitiesFromSnapshot(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, encoded := range values {
		bytes, err := hex.DecodeString(strings.TrimPrefix(encoded, "0x"))
		if err != nil {
			out[key] = encoded
			continue
		}
		printable := true
		for _, b := range bytes {
			if b == '\n' || b == '\r' || b == '\t' {
				continue
			}
			if b < 32 || b > 126 {
				printable = false
				break
			}
		}
		if printable {
			out[key] = string(bytes)
		} else {
			out[key] = encoded
		}
	}
	return out
}

type lotusActorIdentityResolver struct {
	rpcURL     string
	httpClient *http.Client
}

func (r *lotusActorIdentityResolver) ResolveActorID(ctx context.Context, evmAddress common.Address) (string, string, error) {
	filecoinAddress, err := r.callString(ctx, "Filecoin.EthAddressToFilecoinAddress", []any{evmAddress.Hex()})
	if err != nil {
		return "", "", err
	}
	actorID, err := r.callString(ctx, "Filecoin.StateLookupID", []any{filecoinAddress, nil})
	if err != nil {
		return filecoinAddress, "", err
	}
	return filecoinAddress, actorID, nil
}

func (r *lotusActorIdentityResolver) callString(ctx context.Context, method string, params []any) (string, error) {
	var out string
	if err := r.call(ctx, method, params, &out); err != nil {
		return "", err
	}
	return out, nil
}

func (r *lotusActorIdentityResolver) call(ctx context.Context, method string, params []any, out any) error {
	if r.rpcURL == "" {
		return errors.New("empty Filecoin RPC URL")
	}
	body, err := json.Marshal(lotusRPCRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.rpcURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")

	client := r.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, truncated, err := readLimitedRPCBody(resp.Body, maxFilecoinRPCResponseBodyBytes)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		truncatedSuffix := ""
		if truncated {
			truncatedSuffix = fmt.Sprintf(" response body exceeds %d bytes", maxFilecoinRPCResponseBodyBytes)
		}
		return fmt.Errorf("filecoin rpc %s: http %d%s: %s", method, resp.StatusCode, truncatedSuffix, truncateRPCBody(raw, 512))
	}
	if truncated {
		return fmt.Errorf("filecoin rpc %s: response body exceeds %d bytes", method, maxFilecoinRPCResponseBodyBytes)
	}
	var decoded lotusRPCResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	if decoded.Error != nil {
		return fmt.Errorf("filecoin rpc %s: %d %s", method, decoded.Error.Code, decoded.Error.Message)
	}
	if len(decoded.Result) == 0 || string(decoded.Result) == "null" {
		return fmt.Errorf("filecoin rpc %s: empty result", method)
	}
	return json.Unmarshal(decoded.Result, out)
}

func readLimitedRPCBody(body io.Reader, limit int64) ([]byte, bool, error) {
	raw, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(raw)) <= limit {
		return raw, false, nil
	}
	return raw[:limit], true, nil
}

func truncateRPCBody(raw []byte, limit int) string {
	body := string(bytes.TrimSpace(raw))
	if limit <= 0 || len(body) <= limit {
		return body
	}
	return body[:limit] + "...(truncated)"
}

type lotusRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type lotusRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *lotusRPCError  `json:"error"`
}

type lotusRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
