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
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/spregistry"
	sdktypes "github.com/strahe/synapse-go/types"
)

const (
	defaultProviderIdentityTTL             = 5 * time.Minute
	defaultProviderIdentityRPCTimeout      = 5 * time.Second
	defaultProviderIdentityRegistryTimeout = 5 * time.Second
	defaultProviderIdentityActorTimeout    = 2 * time.Second
	defaultProviderIdentityRefreshBackoff  = 60 * time.Second
	defaultProviderIdentityQueueSize       = 128
	defaultProviderIdentityActorWorkers    = 4
	maxFilecoinRPCResponseBodyBytes        = 1 << 20
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
	ProviderIdentities([]idtypes.OnChainID) map[string]*providerIdentityResponse
}

type providerIdentityRunner interface {
	Run(context.Context)
}

type providerIdentityPublisherSetter interface {
	SetProviderIdentityPublisher(func(*providerIdentityResponse))
}

type providerIdentityRegistry interface {
	GetPDPProvidersByIDs(context.Context, []sdktypes.BigInt) ([]spregistry.PDPProvider, error)
	GetProvidersByIDs(context.Context, []sdktypes.BigInt) ([]*spregistry.ProviderInfo, error)
}

type actorIdentityResolver interface {
	ResolveActorID(context.Context, common.Address) (filecoinAddress string, actorID string, err error)
}

type providerIdentityCacheEntry struct {
	identity  *providerIdentityResponse
	expiresAt time.Time
}

// ProviderIdentityResolver asynchronously enriches Registry provider IDs for admin API responses.
type ProviderIdentityResolver struct {
	registry providerIdentityRegistry
	actors   actorIdentityResolver
	ttl      time.Duration
	now      func() time.Time
	logger   *slog.Logger

	registryTimeout time.Duration
	actorTimeout    time.Duration
	refreshBackoff  time.Duration

	refreshQueue chan []idtypes.OnChainID
	actorQueue   chan string

	mu               sync.Mutex
	cache            map[string]providerIdentityCacheEntry
	refreshing       map[string]struct{}
	refreshBackoffs  map[string]time.Time
	actorRefreshing  map[string]struct{}
	actorBackoffs    map[string]time.Time
	publishIdentity  func(*providerIdentityResponse)
	actorWorkerCount int
}

// NewProviderIdentityResolver creates a cached provider identity resolver backed by the Registry and Filecoin RPC.
func NewProviderIdentityResolver(registry *spregistry.Service, rpcURL string, logger *slog.Logger) *ProviderIdentityResolver {
	if registry == nil {
		return nil
	}
	var actors actorIdentityResolver
	if rpcURL != "" {
		actors = &lotusActorIdentityResolver{rpcURL: rpcURL, httpClient: &http.Client{Timeout: defaultProviderIdentityRPCTimeout}}
	}
	return newProviderIdentityResolver(registry, actors, defaultProviderIdentityTTL, time.Now, logger)
}

func newProviderIdentityResolver(
	registry providerIdentityRegistry,
	actors actorIdentityResolver,
	ttl time.Duration,
	now func() time.Time,
	logger *slog.Logger,
) *ProviderIdentityResolver {
	if now == nil {
		now = time.Now
	}
	return &ProviderIdentityResolver{
		registry:         registry,
		actors:           actors,
		ttl:              ttl,
		now:              now,
		logger:           logger,
		registryTimeout:  defaultProviderIdentityRegistryTimeout,
		actorTimeout:     defaultProviderIdentityActorTimeout,
		refreshBackoff:   defaultProviderIdentityRefreshBackoff,
		refreshQueue:     make(chan []idtypes.OnChainID, defaultProviderIdentityQueueSize),
		actorQueue:       make(chan string, defaultProviderIdentityQueueSize),
		cache:            make(map[string]providerIdentityCacheEntry),
		refreshing:       make(map[string]struct{}),
		refreshBackoffs:  make(map[string]time.Time),
		actorRefreshing:  make(map[string]struct{}),
		actorBackoffs:    make(map[string]time.Time),
		actorWorkerCount: defaultProviderIdentityActorWorkers,
	}
}

func (r *ProviderIdentityResolver) SetProviderIdentityPublisher(publish func(*providerIdentityResponse)) {
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
	for i := 0; i < r.actorWorkerCount; i++ {
		go r.runActorWorker(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ids := <-r.refreshQueue:
			r.refreshProviderIdentities(ctx, r.drainRefreshIDs(ids))
		}
	}
}

func (r *ProviderIdentityResolver) ProviderIdentities(providerIDs []idtypes.OnChainID) map[string]*providerIdentityResponse {
	out := make(map[string]*providerIdentityResponse)
	if r == nil || r.registry == nil {
		return out
	}

	seen := make(map[string]struct{}, len(providerIDs))
	refreshIDs := make([]idtypes.OnChainID, 0)
	for _, providerID := range providerIDs {
		if providerID.IsZero() {
			continue
		}
		key := providerID.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		identity, stale := r.cachedSnapshot(key)
		if identity != nil {
			out[key] = identity
			if !stale {
				r.enqueueActor(identity)
			}
		}
		if identity == nil || stale {
			refreshIDs = append(refreshIDs, providerID)
		}
	}
	r.enqueueRefresh(refreshIDs)
	return out
}

func (r *ProviderIdentityResolver) cachedSnapshot(key string) (*providerIdentityResponse, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.cache[key]
	if !ok || entry.identity == nil {
		return nil, false
	}
	stale := !entry.expiresAt.IsZero() && !r.now().Before(entry.expiresAt)
	return cloneProviderIdentity(entry.identity), stale
}

func (r *ProviderIdentityResolver) enqueueRefresh(providerIDs []idtypes.OnChainID) {
	if len(providerIDs) == 0 {
		return
	}

	now := r.now()
	enqueue := make([]idtypes.OnChainID, 0, len(providerIDs))
	r.mu.Lock()
	for _, providerID := range providerIDs {
		key := providerID.String()
		if _, ok := r.refreshing[key]; ok {
			continue
		}
		if until, ok := r.refreshBackoffs[key]; ok && now.Before(until) {
			continue
		}
		r.refreshing[key] = struct{}{}
		enqueue = append(enqueue, providerID)
	}
	r.mu.Unlock()

	if len(enqueue) == 0 {
		return
	}
	select {
	case r.refreshQueue <- enqueue:
	default:
		r.clearRefreshing(enqueue)
		if r.logger != nil {
			r.logger.Debug("provider identity: refresh queue full", "count", len(enqueue))
		}
	}
}

func (r *ProviderIdentityResolver) drainRefreshIDs(first []idtypes.OnChainID) []idtypes.OnChainID {
	ids := make([]idtypes.OnChainID, 0, len(first))
	seen := make(map[string]struct{}, len(first))
	add := func(batch []idtypes.OnChainID) {
		for _, id := range batch {
			key := id.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			ids = append(ids, id)
		}
	}
	add(first)
	for {
		select {
		case batch := <-r.refreshQueue:
			add(batch)
		default:
			return ids
		}
	}
}

func (r *ProviderIdentityResolver) refreshProviderIdentities(ctx context.Context, providerIDs []idtypes.OnChainID) {
	if len(providerIDs) == 0 {
		return
	}
	defer r.clearRefreshing(providerIDs)

	callCtx := ctx
	var cancel context.CancelFunc
	if r.registryTimeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, r.registryTimeout)
		defer cancel()
	}

	sdkIDs := make([]sdktypes.BigInt, 0, len(providerIDs))
	requested := make(map[string]idtypes.OnChainID, len(providerIDs))
	for _, providerID := range providerIDs {
		key := providerID.String()
		requested[key] = providerID
		sdkIDs = append(sdkIDs, providerID.SDK())
	}

	pdpProviders, err := r.registry.GetPDPProvidersByIDs(callCtx, sdkIDs)
	if err != nil {
		r.markRefreshBackoff(providerIDs)
		if r.logger != nil {
			r.logger.Debug("provider identity: failed to refresh PDP providers", "count", len(providerIDs), "error", err)
		}
		return
	}

	found := make(map[string]struct{}, len(pdpProviders))
	for _, provider := range pdpProviders {
		key := provider.Info.ID.String()
		if _, ok := requested[key]; !ok || provider.Info.ID.IsZero() {
			continue
		}
		found[key] = struct{}{}
		identity := identityFromPDPProvider(&provider)
		r.enqueueActor(r.storeAndPublish(identity))
	}

	missing := make([]idtypes.OnChainID, 0)
	for key, providerID := range requested {
		if _, ok := found[key]; !ok {
			missing = append(missing, providerID)
		}
	}
	r.refreshProviderInfoFallback(ctx, missing)
}

func (r *ProviderIdentityResolver) refreshProviderInfoFallback(ctx context.Context, providerIDs []idtypes.OnChainID) {
	if len(providerIDs) == 0 {
		return
	}
	callCtx := ctx
	var cancel context.CancelFunc
	if r.registryTimeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, r.registryTimeout)
		defer cancel()
	}
	sdkIDs := make([]sdktypes.BigInt, 0, len(providerIDs))
	for _, providerID := range providerIDs {
		sdkIDs = append(sdkIDs, providerID.SDK())
	}
	infos, err := r.registry.GetProvidersByIDs(callCtx, sdkIDs)
	if err != nil {
		r.markRefreshBackoff(providerIDs)
		if r.logger != nil {
			r.logger.Debug("provider identity: failed to refresh provider info fallback", "count", len(providerIDs), "error", err)
		}
		return
	}
	if len(infos) != len(providerIDs) {
		r.markRefreshBackoff(providerIDs)
		if r.logger != nil {
			r.logger.Debug("provider identity: malformed provider info fallback", "got", len(infos), "want", len(providerIDs))
		}
		return
	}
	missing := make([]idtypes.OnChainID, 0)
	for i, info := range infos {
		if info == nil {
			missing = append(missing, providerIDs[i])
			continue
		}
		identity := identityFromProviderInfo(info)
		r.enqueueActor(r.storeAndPublish(identity))
	}
	r.markRefreshBackoff(missing)
}

func (r *ProviderIdentityResolver) clearRefreshing(providerIDs []idtypes.OnChainID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, providerID := range providerIDs {
		delete(r.refreshing, providerID.String())
	}
}

func (r *ProviderIdentityResolver) markRefreshBackoff(providerIDs []idtypes.OnChainID) {
	if len(providerIDs) == 0 || r.refreshBackoff <= 0 {
		return
	}
	until := r.now().Add(r.refreshBackoff)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, providerID := range providerIDs {
		r.refreshBackoffs[providerID.String()] = until
	}
}

func (r *ProviderIdentityResolver) storeAndPublish(identity *providerIdentityResponse) *providerIdentityResponse {
	stored := r.store(identity)
	if stored == nil {
		return nil
	}

	r.mu.Lock()
	publish := r.publishIdentity
	r.mu.Unlock()
	if publish != nil {
		publish(cloneProviderIdentity(stored))
	}
	return stored
}

func (r *ProviderIdentityResolver) store(identity *providerIdentityResponse) *providerIdentityResponse {
	if identity == nil || identity.RegistryProviderID == "" {
		return nil
	}
	stored := cloneProviderIdentity(identity)
	expiresAt := time.Time{}
	if r.ttl > 0 {
		expiresAt = r.now().Add(r.ttl)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if current := r.cache[stored.RegistryProviderID].identity; current != nil {
		if sameServiceProviderAddress(stored.ServiceProviderAddress, current.ServiceProviderAddress) {
			if stored.FilecoinAddress == "" {
				stored.FilecoinAddress = current.FilecoinAddress
			}
			if stored.FilecoinActorID == "" {
				stored.FilecoinActorID = current.FilecoinActorID
			}
		} else {
			delete(r.actorBackoffs, stored.RegistryProviderID)
		}
	}
	r.cache[stored.RegistryProviderID] = providerIdentityCacheEntry{identity: stored, expiresAt: expiresAt}
	delete(r.refreshBackoffs, stored.RegistryProviderID)
	return cloneProviderIdentity(stored)
}

func sameServiceProviderAddress(a, b string) bool {
	if common.IsHexAddress(a) && common.IsHexAddress(b) {
		return common.HexToAddress(a) == common.HexToAddress(b)
	}
	return a == b
}

func (r *ProviderIdentityResolver) enqueueActor(identity *providerIdentityResponse) {
	if r == nil || r.actors == nil || identity == nil || identity.RegistryProviderID == "" || identity.FilecoinActorID != "" {
		return
	}
	if !common.IsHexAddress(identity.ServiceProviderAddress) {
		return
	}

	key := identity.RegistryProviderID
	now := r.now()
	r.mu.Lock()
	if _, ok := r.actorRefreshing[key]; ok {
		r.mu.Unlock()
		return
	}
	if until, ok := r.actorBackoffs[key]; ok && now.Before(until) {
		r.mu.Unlock()
		return
	}
	r.actorRefreshing[key] = struct{}{}
	r.mu.Unlock()

	select {
	case r.actorQueue <- key:
	default:
		r.mu.Lock()
		delete(r.actorRefreshing, key)
		r.mu.Unlock()
		if r.logger != nil {
			r.logger.Debug("provider identity: actor queue full", "provider_id", key)
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

func (r *ProviderIdentityResolver) resolveActor(ctx context.Context, key string) {
	defer func() {
		r.mu.Lock()
		delete(r.actorRefreshing, key)
		r.mu.Unlock()
	}()

	identity := r.identityForActorLookup(key)
	if identity == nil || !common.IsHexAddress(identity.ServiceProviderAddress) {
		return
	}

	callCtx := ctx
	var cancel context.CancelFunc
	if r.actorTimeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, r.actorTimeout)
		defer cancel()
	}
	filecoinAddress, actorID, err := r.actors.ResolveActorID(callCtx, common.HexToAddress(identity.ServiceProviderAddress))
	if err != nil {
		if filecoinAddress != "" {
			enriched := cloneProviderIdentity(identity)
			enriched.FilecoinAddress = filecoinAddress
			r.storeAndPublish(enriched)
		}
		r.markActorBackoff(key)
		if r.logger != nil {
			r.logger.Debug("provider identity: failed to resolve actor id", "provider_id", key, "error", err)
		}
		return
	}

	enriched := cloneProviderIdentity(identity)
	enriched.FilecoinAddress = filecoinAddress
	enriched.FilecoinActorID = actorID
	r.storeAndPublish(enriched)
	r.clearActorBackoff(key)
}

func (r *ProviderIdentityResolver) identityForActorLookup(key string) *providerIdentityResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.cache[key]
	return cloneProviderIdentity(entry.identity)
}

func (r *ProviderIdentityResolver) markActorBackoff(key string) {
	if r.refreshBackoff <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actorBackoffs[key] = r.now().Add(r.refreshBackoff)
}

func (r *ProviderIdentityResolver) clearActorBackoff(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.actorBackoffs, key)
}

func identityFromPDPProvider(provider *spregistry.PDPProvider) *providerIdentityResponse {
	identity := identityFromProviderInfo(&provider.Info)
	identity.ServiceURL = provider.Offering.ServiceURL
	identity.Location = provider.Offering.Location
	identity.ExtraCapabilities = formatExtraCapabilities(provider.Offering.ExtraCapabilities)
	return identity
}

func identityFromProviderInfo(info *spregistry.ProviderInfo) *providerIdentityResponse {
	if info == nil {
		return nil
	}
	return &providerIdentityResponse{
		RegistryProviderID:     info.ID.String(),
		Name:                   info.Name,
		Description:            info.Description,
		ServiceProviderAddress: nonZeroAddressHex(info.ServiceProvider),
		PayeeAddress:           nonZeroAddressHex(info.Payee),
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

func nonZeroAddressHex(addr common.Address) string {
	if addr == (common.Address{}) {
		return ""
	}
	return addr.Hex()
}

func formatExtraCapabilities(extra map[string][]byte) map[string]string {
	if len(extra) == 0 {
		return nil
	}
	out := make(map[string]string, len(extra))
	for key, value := range extra {
		out[key] = formatCapabilityValue(value)
	}
	return out
}

func formatCapabilityValue(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	for _, b := range value {
		if b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		if b < 32 || b > 126 {
			return "0x" + hex.EncodeToString(value)
		}
	}
	return string(value)
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
