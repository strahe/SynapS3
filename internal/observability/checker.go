package observability

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
)

const (
	defaultTimeout     = 5 * time.Second
	defaultConcurrency = 8

	minOperationTimeout       = 30 * time.Second
	maxSingleOperationTimeout = 2 * time.Minute
	maxFanoutOperationTimeout = 10 * time.Minute
)

type ProviderSource interface {
	ListActiveProviders(context.Context) ([]Provider, error)
	LookupProvider(context.Context, types.OnChainID) (Provider, error)
}

type ProviderHealthFunc func(context.Context, string, time.Duration) string

type DataSetScanner interface {
	ScanWalletDataSets(context.Context) ([]ChainDataSet, error)
}

type CheckerOptions struct {
	ProviderSource ProviderSource
	ProviderHealth ProviderHealthFunc
	DataSetScanner DataSetScanner
	Timeout        time.Duration
	Concurrency    int
	Now            func() time.Time
	Logger         *slog.Logger
}

type Checker struct {
	providerSource ProviderSource
	providerHealth ProviderHealthFunc
	dataSetScanner DataSetScanner
	timeout        time.Duration
	concurrency    int
	now            func() time.Time
	logger         *slog.Logger
}

func NewChecker(opts CheckerOptions) *Checker {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Checker{
		providerSource: opts.ProviderSource,
		providerHealth: opts.ProviderHealth,
		dataSetScanner: opts.DataSetScanner,
		timeout:        timeout,
		concurrency:    concurrency,
		now:            now,
		logger:         opts.Logger,
	}
}

func (c *Checker) CheckProviders(ctx context.Context, checkedAt time.Time, localDataSets []LocalDataSet) ([]ProviderState, error) {
	if checkedAt.IsZero() {
		checkedAt = c.checkedAt()
	}
	providerIDs := make(map[string]types.OnChainID)
	providers := make(map[string]Provider)
	providerErrors := make(map[string]string)

	for _, dataSet := range localDataSets {
		if dataSet.ProviderID.IsZero() {
			continue
		}
		providerIDs[dataSet.ProviderID.String()] = dataSet.ProviderID
	}

	if c.providerSource != nil {
		listCtx, cancel := context.WithTimeout(ctx, boundedSingleTimeout(c.timeout))
		active, err := c.providerSource.ListActiveProviders(listCtx)
		cancel()
		if err != nil {
			if c.logger != nil {
				c.logger.Warn("observability provider registry list failed", "error", safeObservabilityError(err))
			}
			return nil, err
		}
		for _, provider := range active {
			if provider.ID.IsZero() {
				continue
			}
			key := provider.ID.String()
			providerIDs[key] = provider.ID
			providers[key] = provider
		}
	}

	c.lookupMissingProviders(ctx, providerIDs, providers, providerErrors)

	healthCtx, healthCancel := context.WithTimeout(ctx, boundedFanoutTimeout(c.timeout, len(providers), c.concurrency))
	health := c.checkProviderHealth(healthCtx, providers)
	healthCancel()
	states := make([]ProviderState, 0, len(providerIDs))
	for key, id := range providerIDs {
		if errText, ok := providerErrors[key]; ok {
			states = append(states, ProviderState{
				ProviderID:    id,
				Status:        StatusUnknown,
				ReasonCodes:   []ReasonCode{ReasonRegistryLookupFailed},
				LastCheckedAt: checkedAt,
				LastError:     &errText,
				Evidence:      map[string]any{},
			})
			continue
		}

		provider := providers[key]
		status := StatusAvailable
		reasons := make([]ReasonCode, 0, 2)
		if !provider.Active {
			status = worseStatus(status, StatusUnavailable)
			reasons = append(reasons, ReasonProviderInactive)
		}
		if !provider.HasPDP {
			status = worseStatus(status, StatusUnavailable)
			if !reasonCodeContains(reasons, ReasonProviderMissingPDP) {
				reasons = append(reasons, ReasonProviderMissingPDP)
			}
		}
		healthStatus := health[key]
		if healthStatus == "" {
			healthStatus = provider.HealthStatus
		}
		if healthStatus == "" {
			healthStatus = "unknown"
		}
		if provider.HasPDP && (provider.ServiceURL == "" || healthStatus == "n/a" || healthStatus == "unreachable") {
			status = worseStatus(status, StatusDegraded)
			if !reasonCodeContains(reasons, ReasonProviderHTTPUnreachable) {
				reasons = append(reasons, ReasonProviderHTTPUnreachable)
			}
		}

		states = append(states, ProviderState{
			ProviderID:    id,
			Status:        status,
			ReasonCodes:   reasons,
			Active:        boolPtr(provider.Active),
			HasPDP:        boolPtr(provider.HasPDP),
			ServiceURL:    stringPtr(provider.ServiceURL),
			HealthStatus:  stringPtr(healthStatus),
			LastCheckedAt: checkedAt,
			Evidence: map[string]any{
				"service_url": provider.ServiceURL,
				"has_pdp":     provider.HasPDP,
			},
		})
	}

	sort.Slice(states, func(i, j int) bool {
		return lessOnChainID(states[i].ProviderID, states[j].ProviderID)
	})
	return states, nil
}

func (c *Checker) CheckDataSets(ctx context.Context, checkedAt time.Time, localDataSets []LocalDataSet) ([]DataSetState, error) {
	if checkedAt.IsZero() {
		checkedAt = c.checkedAt()
	}
	needsChain := false
	for _, local := range localDataSets {
		if local.DataSetID != nil && !local.DataSetID.IsZero() {
			needsChain = true
			break
		}
	}

	var chainDataSets []ChainDataSet
	var chainErr error
	if needsChain {
		if c.dataSetScanner == nil {
			chainErr = ErrChainScannerUnavailable
		} else {
			scanCtx, cancel := context.WithTimeout(ctx, boundedSingleTimeout(c.timeout))
			chainDataSets, chainErr = c.dataSetScanner.ScanWalletDataSets(scanCtx)
			cancel()
		}
	}
	chainByID := make(map[string]ChainDataSet, len(chainDataSets))
	for _, chainDataSet := range chainDataSets {
		if chainDataSet.DataSetID.IsZero() {
			continue
		}
		chainByID[chainDataSet.DataSetID.String()] = chainDataSet
	}
	matchedChainIDs := make(map[string]struct{}, len(localDataSets))

	states := make([]DataSetState, 0, len(localDataSets))
	for _, local := range localDataSets {
		state := DataSetState{
			LocalDataSetID:  local.ID,
			BucketID:        local.BucketID,
			BucketName:      local.BucketName,
			CopyIndex:       local.CopyIndex,
			ProviderID:      local.ProviderID,
			ChainDataSetID:  local.DataSetID,
			ClientDataSetID: local.ClientDataSetID,
			LocalStatus:     local.Status,
			Status:          StatusAvailable,
			ReasonCodes:     make([]ReasonCode, 0, 3),
			LastCheckedAt:   checkedAt,
			Evidence:        map[string]any{},
		}

		if local.DataSetID == nil || local.DataSetID.IsZero() {
			state.Status = StatusUnavailable
			state.ReasonCodes = append(state.ReasonCodes, ReasonChainDataSetMissing)
			states = append(states, state)
			continue
		}
		if chainErr != nil {
			errText := safeObservabilityError(chainErr)
			state.Status = StatusUnknown
			state.ReasonCodes = append(state.ReasonCodes, ReasonChainLookupFailed)
			state.LastError = &errText
			if localSeverity := localStatusSeverity(local.Status); localSeverity != StatusAvailable {
				if localSeverity == StatusUnavailable {
					state.Status = StatusUnavailable
				}
				state.ReasonCodes = append(state.ReasonCodes, ReasonLocalStatusNotReady)
			}
			states = append(states, state)
			continue
		}
		chainDataSet, ok := chainByID[local.DataSetID.String()]
		if !ok {
			state.Status = StatusUnavailable
			state.ReasonCodes = append(state.ReasonCodes, ReasonChainDataSetMissing)
			states = append(states, state)
			continue
		}
		matchedChainIDs[local.DataSetID.String()] = struct{}{}

		state.ActivePieceCount = chainDataSet.ActivePieceCount
		state.Evidence = chainEvidence(chainDataSet)
		if !chainDataSet.IsLive {
			state.Status = worseStatus(state.Status, StatusUnavailable)
			state.ReasonCodes = append(state.ReasonCodes, ReasonChainDataSetInactive)
		}
		if !chainDataSet.IsManaged {
			state.Status = worseStatus(state.Status, StatusDegraded)
			state.ReasonCodes = append(state.ReasonCodes, ReasonChainDataSetUnmanaged)
		}
		if !chainDataSet.ProviderID.IsZero() && !chainDataSet.ProviderID.Equal(local.ProviderID) {
			state.Status = worseStatus(state.Status, StatusDegraded)
			state.ReasonCodes = append(state.ReasonCodes, ReasonProviderMismatch)
		}
		if bucketName, ok := chainDataSet.Metadata["bucket"]; ok && bucketName != "" && bucketName != local.BucketName {
			state.Status = worseStatus(state.Status, StatusDegraded)
			state.ReasonCodes = append(state.ReasonCodes, ReasonMetadataMismatch)
		}
		if localStatusSeverity(local.Status) != StatusAvailable {
			localSeverity := localStatusSeverity(local.Status)
			state.Status = worseStatus(state.Status, localSeverity)
			state.ReasonCodes = append(state.ReasonCodes, ReasonLocalStatusNotReady)
		}
		states = append(states, state)
	}
	c.logUnmatchedChainDataSets(chainByID, matchedChainIDs)

	sort.Slice(states, func(i, j int) bool {
		return states[i].LocalDataSetID < states[j].LocalDataSetID
	})
	return states, nil
}

func (c *Checker) lookupMissingProviders(
	ctx context.Context,
	providerIDs map[string]types.OnChainID,
	providers map[string]Provider,
	providerErrors map[string]string,
) {
	missing := make([]types.OnChainID, 0)
	for key, id := range providerIDs {
		if _, ok := providers[key]; ok {
			continue
		}
		missing = append(missing, id)
	}
	if len(missing) == 0 {
		return
	}
	sort.Slice(missing, func(i, j int) bool {
		return lessOnChainID(missing[i], missing[j])
	})
	if c.providerSource == nil {
		for _, id := range missing {
			providerErrors[id.String()] = safeObservabilityError(ErrProviderNotFound)
		}
		return
	}

	lookupCtx, cancel := context.WithTimeout(ctx, boundedFanoutTimeout(c.timeout, len(missing), c.concurrency))
	defer cancel()

	type lookupResult struct {
		key      string
		provider Provider
		err      error
	}
	results := make(chan lookupResult, len(missing))
	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup
	for _, id := range missing {
		select {
		case sem <- struct{}{}:
		case <-lookupCtx.Done():
			continue
		}
		wg.Add(1)
		go func(id types.OnChainID) {
			defer wg.Done()
			defer func() { <-sem }()
			provider, err := c.providerSource.LookupProvider(lookupCtx, id)
			results <- lookupResult{key: id.String(), provider: provider, err: err}
		}(id)
	}
	wg.Wait()
	close(results)

	for result := range results {
		if result.err != nil {
			providerErrors[result.key] = safeObservabilityError(result.err)
			continue
		}
		providers[result.key] = result.provider
	}

	if err := lookupCtx.Err(); err != nil {
		errText := safeObservabilityError(err)
		for _, id := range missing {
			key := id.String()
			if _, ok := providers[key]; ok {
				continue
			}
			if _, ok := providerErrors[key]; ok {
				continue
			}
			providerErrors[key] = errText
		}
	}
}

func (c *Checker) logUnmatchedChainDataSets(chainByID map[string]ChainDataSet, matched map[string]struct{}) {
	if c == nil || c.logger == nil || len(chainByID) == 0 {
		return
	}
	var count int
	samples := make([]string, 0, 5)
	for key, chainDataSet := range chainByID {
		if _, ok := matched[key]; ok {
			continue
		}
		count++
		if len(samples) < cap(samples) {
			samples = append(samples, chainDataSet.DataSetID.String())
		}
	}
	if count == 0 {
		return
	}
	c.logger.Debug(
		"observability wallet data sets ignored without local binding",
		"count", count,
		"sample_chain_data_set_ids", samples,
	)
}

var ErrChainScannerUnavailable = errString("chain data set scanner unavailable")

type errString string

func (e errString) Error() string { return string(e) }

func (c *Checker) checkedAt() time.Time {
	if c == nil || c.now == nil {
		return time.Now().UTC()
	}
	return c.now().UTC()
}

func (c *Checker) checkProviderHealth(ctx context.Context, providers map[string]Provider) map[string]string {
	out := make(map[string]string, len(providers))
	if c.providerHealth == nil || len(providers) == 0 {
		return out
	}

	ids := make([]string, 0, len(providers))
	for key := range providers {
		ids = append(ids, key)
	}
	sort.Slice(ids, func(i, j int) bool {
		return lessDecimalString(ids[i], ids[j])
	})

	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, key := range ids {
		provider := providers[key]
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return out
		}
		wg.Add(1)
		go func(key string, provider Provider) {
			defer wg.Done()
			defer func() { <-sem }()
			status := c.providerHealth(ctx, provider.ServiceURL, c.timeout)
			mu.Lock()
			out[key] = status
			mu.Unlock()
		}(key, provider)
	}
	wg.Wait()
	return out
}

func boundedSingleTimeout(timeout time.Duration) time.Duration {
	return clampDuration(timeout, minOperationTimeout, maxSingleOperationTimeout)
}

func boundedFanoutTimeout(timeout time.Duration, items int, concurrency int) time.Duration {
	if items < 1 {
		items = 1
	}
	if concurrency < 1 {
		concurrency = 1
	}
	waves := (items + concurrency - 1) / concurrency
	return clampDuration(timeout*time.Duration(waves+1), minOperationTimeout, maxFanoutOperationTimeout)
}

func clampDuration(value, min, max time.Duration) time.Duration {
	if value <= 0 {
		value = defaultTimeout
	}
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func safeObservabilityError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "request cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "request timed out"
	default:
		return "RPC call failed"
	}
}

func lessOnChainID(a, b types.OnChainID) bool {
	return lessDecimalString(a.String(), b.String())
}

func lessDecimalString(a, b string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

func reasonCodeContains(codes []ReasonCode, want ReasonCode) bool {
	for _, code := range codes {
		if code == want {
			return true
		}
	}
	return false
}

func worseStatus(current, next Status) Status {
	if statusRank(next) > statusRank(current) {
		return next
	}
	return current
}

func statusRank(status Status) int {
	switch status {
	case StatusUnavailable:
		return 3
	case StatusDegraded:
		return 2
	case StatusUnknown:
		return 1
	default:
		return 0
	}
}

func localStatusSeverity(status model.StorageDataSetStatus) Status {
	switch status {
	case model.StorageDataSetStatusReady, model.StorageDataSetStatusDraining:
		return StatusAvailable
	case model.StorageDataSetStatusPending, model.StorageDataSetStatusCreating:
		return StatusDegraded
	case model.StorageDataSetStatusFailed, model.StorageDataSetStatusUnavailable, model.StorageDataSetStatusRetired:
		return StatusUnavailable
	default:
		return StatusDegraded
	}
}

func chainEvidence(dataSet ChainDataSet) map[string]any {
	evidence := map[string]any{
		"is_live":    dataSet.IsLive,
		"is_managed": dataSet.IsManaged,
		"metadata":   dataSet.Metadata,
	}
	if !dataSet.ProviderID.IsZero() {
		evidence["provider_id"] = dataSet.ProviderID.String()
	}
	if dataSet.ClientDataSetID != nil && !dataSet.ClientDataSetID.IsZero() {
		evidence["client_data_set_id"] = dataSet.ClientDataSetID.String()
	}
	if dataSet.ActivePieceCount != nil {
		evidence["active_piece_count"] = *dataSet.ActivePieceCount
	}
	return evidence
}

func boolPtr(value bool) *bool {
	return &value
}

func stringPtr(value string) *string {
	return &value
}

func (r ReasonCode) String() string {
	return string(r)
}
