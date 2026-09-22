package task

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

type EngineConfig struct {
	Concurrency                    int
	PollInterval                   time.Duration
	LeaseDuration                  time.Duration
	Retention                      time.Duration
	ProviderMutationConcurrency    int
	DestructiveMutationConcurrency int
	OnTaskSettled                  func(*model.Task, repository.TaskTransition)
}

const (
	retryBaseDelay           = 10 * time.Second
	retryMaximumDelay        = 5 * time.Minute
	resourceWaitBaseDelay    = 2 * time.Second
	resourceWaitMaximumDelay = time.Minute
	backoffJitterFraction    = 0.20
)

// Engine is the only task claimant and lease owner.
type Engine struct {
	config                EngineConfig
	repos                 *repository.Repositories
	registry              *Registry
	logger                *slog.Logger
	gates                 map[Resource]chan struct{}
	recoveryMu            sync.Mutex
	recovery              map[recoveryRequest]struct{}
	recoveryWake          chan struct{}
	settlementRetryDelays []time.Duration
	renewalRetryDelays    []time.Duration
	retryDelay            func(int) time.Duration
	resourceWaitDelay     func(int) time.Duration
	resourceWaitMu        sync.Mutex
	// resourceWaits counts each task's consecutive resource waits. It is kept
	// in memory only; a restart merely restarts the backoff.
	resourceWaits map[int64]int
	lastTick      atomic.Int64
}

type recoveryRequest struct {
	id         int64
	generation int64
}

func NewEngine(config EngineConfig, repos *repository.Repositories, registry *Registry, logger *slog.Logger) (*Engine, error) {
	if config.Concurrency < 1 || config.PollInterval <= 0 || config.LeaseDuration <= config.PollInterval ||
		config.Retention <= 0 || config.ProviderMutationConcurrency < 1 || config.DestructiveMutationConcurrency < 1 {
		return nil, errors.New("invalid task engine configuration")
	}
	if repos == nil || repos.Tasks == nil || registry == nil {
		return nil, errors.New("task engine requires repositories and registry")
	}
	if logger == nil {
		logger = slog.Default()
	}
	registry.freeze()
	return &Engine{
		config: config, repos: repos, registry: registry, logger: logger,
		gates: map[Resource]chan struct{}{
			ResourceProviderMutation:    make(chan struct{}, config.ProviderMutationConcurrency),
			ResourceProviderUploadSpeed: make(chan struct{}, 1),
			ResourceDestructiveMutation: make(chan struct{}, config.DestructiveMutationConcurrency),
			ResourceWallet:              make(chan struct{}, 1),
		},
		recovery:              make(map[recoveryRequest]struct{}),
		recoveryWake:          make(chan struct{}, 1),
		settlementRetryDelays: []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second},
		renewalRetryDelays:    []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second},
		retryDelay:            defaultRetryDelay,
		resourceWaitDelay:     defaultResourceWaitDelay,
		resourceWaits:         make(map[int64]int),
	}, nil
}

func (e *Engine) Name() string { return "tasks" }

func (e *Engine) Healthy() bool {
	last := time.Unix(0, e.lastTick.Load())
	return !last.IsZero() && time.Since(last) <= maxDuration(3*e.config.PollInterval, time.Minute)
}

func (e *Engine) Run(ctx context.Context) error {
	var workers sync.WaitGroup
	for range e.config.Concurrency {
		workers.Go(func() {
			e.runSlot(ctx)
		})
	}
	workers.Go(func() {
		e.runRecoveryQueue(ctx)
	})
	workers.Wait()
	return nil
}

func (e *Engine) runSlot(ctx context.Context) {
	for ctx.Err() == nil {
		e.lastTick.Store(time.Now().UnixNano())
		claimed, err := e.repos.Tasks.ClaimNext(ctx, e.config.LeaseDuration)
		if err != nil {
			if ctx.Err() == nil {
				e.logger.Error("claiming task", "error", err)
			}
			if !sleepContext(ctx, e.config.PollInterval) {
				return
			}
			continue
		}
		if claimed == nil {
			if !sleepContext(ctx, e.config.PollInterval) {
				return
			}
			continue
		}
		e.executeClaimSafely(ctx, claimed)
	}
}

func (e *Engine) executeClaimSafely(parent context.Context, claimed *model.Task) {
	defer func() {
		if recovered := recover(); recovered != nil {
			e.logger.Error("task execution panicked outside the handler boundary",
				"task_id", claimed.ID,
				"task_type", claimed.Type,
				"claim_generation", claimed.ClaimGeneration,
				"error", recovered,
				"stack", string(debug.Stack()),
			)
			e.abandonClaim(claimed)
		}
	}()
	e.executeClaim(parent, claimed)
}

func (e *Engine) executeClaim(parent context.Context, claimed *model.Task) {
	logger := e.logger.With("task_id", claimed.ID, "task_type", claimed.Type, "claim_generation", claimed.ClaimGeneration)
	handler, ok := e.registry.Handler(claimed.Type)
	if !ok {
		logger.Error("claimed task has no registered handler")
		if e.failClaim(parent, claimed, "handler_unavailable", fmt.Errorf("no handler is registered for task type %q", claimed.Type)) != nil {
			e.abandonClaim(claimed)
		}
		return
	}
	definition, ok := e.registry.Definition(claimed.Type)
	if !ok {
		logger.Error("claimed task definition is unavailable")
		if e.failClaim(parent, claimed, "handler_unavailable", fmt.Errorf("no definition is registered for task type %q", claimed.Type)) != nil {
			e.abandonClaim(claimed)
		}
		return
	}
	if claimed.InputVersion != definition.InputVersion {
		logger.Error("claimed task input version is unsupported", "input_version", claimed.InputVersion, "current_version", definition.InputVersion)
		if e.failClaim(parent, claimed, "input_version_unsupported", fmt.Errorf("task input version %d is unsupported", claimed.InputVersion)) != nil {
			e.abandonClaim(claimed)
		}
		return
	}
	canonical, err := canonicalizeInput(definition.Codec, claimed.Input)
	if err != nil {
		logger.Error("claimed task input is invalid", "error", err)
		reason := "invalid_input"
		if errors.Is(err, ErrCodecPanic) {
			reason = "input_codec_panic"
		}
		if e.failClaim(parent, claimed, reason, err) != nil {
			e.abandonClaim(claimed)
		}
		return
	}
	inputSum := sha256.Sum256(canonical)
	if !bytes.Equal(inputSum[:], decodeHash(claimed.InputHash)) {
		err := errors.New("stored task input hash does not match its canonical input")
		logger.Error("claimed task input hash is invalid")
		if e.failClaim(parent, claimed, "invalid_input_hash", err) != nil {
			e.abandonClaim(claimed)
		}
		return
	}
	// PostgreSQL JSONB may normalize whitespace and key order. Handler input is
	// always the codec's canonical representation after its hash is verified.
	claimed.Input = canonical

	handlerCtx, cancelHandler := context.WithCancel(parent)
	defer cancelHandler()
	var leaseSafe atomic.Bool
	leaseSafe.Store(true)
	stopRenewal := make(chan struct{})
	renewalStopped := make(chan struct{})
	go e.renewLease(handlerCtx, claimed, &leaseSafe, cancelHandler, stopRenewal, renewalStopped)
	renewalDone := false
	stopLeaseRenewal := func() {
		if renewalDone {
			return
		}
		close(stopRenewal)
		<-renewalStopped
		renewalDone = true
	}
	defer stopLeaseRenewal()

	execution := Execution{
		task: *claimed,
		checkpoint: func(ctx context.Context, value any, settlement Settlement) error {
			if !leaseSafe.Load() {
				return repository.ErrTaskLeaseLost
			}
			checkpoint, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("encoding checkpoint: %w", err)
			}
			return e.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
				if err := txRepos.Tasks.ValidateClaim(ctx, claimed.ID, claimed.ClaimGeneration); err != nil {
					return err
				}
				if settlement != nil {
					if err := invokeSettlement(ctx, settlement, txRepos); err != nil {
						return err
					}
				}
				return txRepos.Tasks.WriteCheckpoint(ctx, claimed.ID, claimed.ClaimGeneration, checkpoint)
			})
		},
		resource: func(ctx context.Context, resource Resource, fn func(context.Context) error) error {
			if !leaseSafe.Load() {
				return repository.ErrTaskLeaseLost
			}
			return e.withResource(ctx, resource, func(ctx context.Context) error {
				if !leaseSafe.Load() {
					return repository.ErrTaskLeaseLost
				}
				if err := e.repos.Tasks.ValidateClaim(ctx, claimed.ID, claimed.ClaimGeneration); err != nil {
					return err
				}
				if !leaseSafe.Load() {
					return repository.ErrTaskLeaseLost
				}
				return fn(ctx)
			})
		},
	}

	result, panicked := invokeHandler(handlerCtx, handler, execution)
	if panicked != nil {
		logger.Error("task handler panicked", "error", panicked, "stack", string(debug.Stack()))
		stopLeaseRenewal()
		if e.failClaim(parent, claimed, "handler_panic", fmt.Errorf("task handler panicked: %v", panicked)) != nil {
			e.abandonClaim(claimed)
		}
		return
	}
	if parent.Err() != nil || !leaseSafe.Load() {
		logger.Warn("task result discarded because the lease is uncertain")
		stopLeaseRenewal()
		e.abandonClaim(claimed)
		return
	}
	if err := validateResult(result, claimed); err != nil {
		logger.Error("task handler returned an invalid result", "error", err)
		stopLeaseRenewal()
		if e.failClaim(parent, claimed, "invalid_result", err) != nil {
			e.abandonClaim(claimed)
		}
		return
	}
	stopLeaseRenewal()
	if err := e.commitResult(parent, claimed, result); err != nil {
		logger.Error("settling task result", "error", err)
		e.abandonClaim(claimed)
	}
}

func (e *Engine) failClaim(ctx context.Context, claimed *model.Task, reason string, cause error) error {
	var settlement Settlement
	if definition, ok := e.registry.Definition(claimed.Type); ok && definition.OnEngineFailure != nil {
		settlement = definition.OnEngineFailure(claimed, reason)
	}
	if err := e.commitResult(ctx, claimed, Fail(cause, reason, settlement)); err != nil {
		e.logger.Error("recording task engine failure", "task_id", claimed.ID, "claim_generation", claimed.ClaimGeneration, "error", err)
		return err
	}
	return nil
}

func invokeHandler(ctx context.Context, handler Handler, execution Execution) (result Result, panicValue any) {
	defer func() {
		panicValue = recover()
	}()
	if execution.Mode() == model.TaskResumeModeRecover {
		return handler.Recover(ctx, execution), nil
	}
	return handler.Execute(ctx, execution), nil
}

func validateResult(result Result, _ *model.Task) error {
	switch result.kind {
	case resultComplete:
		return nil
	case resultFail:
		if result.err == nil || result.failureReason == "" {
			return ErrInvalidResult
		}
		return nil
	case resultCancel:
		return nil
	case resultSuspend:
		if result.delay < 0 || (result.resumeMode != model.TaskResumeModeExecute && result.resumeMode != model.TaskResumeModeRecover) {
			return ErrInvalidResult
		}
		return nil
	case resultRetry:
		if result.delay < 0 || result.err == nil || result.failureReason == "" {
			return ErrInvalidResult
		}
		return nil
	default:
		return ErrInvalidResult
	}
}

func (e *Engine) commitResult(ctx context.Context, claimed *model.Task, result Result) error {
	transition := e.transitionFor(claimed, result)
	var lastErr error
	for _, delay := range e.settlementRetryDelays {
		if delay > 0 {
			if err := e.waitSettlementRetry(ctx, claimed, delay); err != nil {
				return err
			}
		}
		leaseUntil, err := e.repos.Tasks.RenewLease(ctx, claimed.ID, claimed.ClaimGeneration, e.config.LeaseDuration)
		if err != nil {
			return err
		}
		settlementDeadline := leaseUntil.Add(-e.config.LeaseDuration / 3)
		if !time.Now().Before(settlementDeadline) {
			return repository.ErrTaskLeaseLost
		}
		settlementCtx, cancel := context.WithDeadline(ctx, settlementDeadline)
		lastErr = e.repos.WithTx(settlementCtx, func(txRepos *repository.Repositories) error {
			if err := txRepos.Tasks.ValidateClaim(settlementCtx, claimed.ID, claimed.ClaimGeneration); err != nil {
				return err
			}
			if result.settlement != nil {
				if err := invokeSettlement(settlementCtx, result.settlement, txRepos); err != nil {
					return err
				}
			}
			return txRepos.Tasks.Settle(settlementCtx, claimed.ID, claimed.ClaimGeneration, transition)
		})
		cancel()
		if lastErr == nil {
			e.recordResourceWait(claimed.ID, result.resourceWait)
			e.notifyTaskSettled(claimed, transition)
			return nil
		}
		if errors.Is(lastErr, repository.ErrTaskLeaseLost) {
			return lastErr
		}
	}
	return lastErr
}

func (e *Engine) notifyTaskSettled(claimed *model.Task, transition repository.TaskTransition) {
	if e.config.OnTaskSettled == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			e.logger.Error("task settlement callback panicked", "task_id", claimed.ID, "panic", recovered)
		}
	}()
	e.config.OnTaskSettled(claimed, transition)
}

func (e *Engine) waitSettlementRetry(ctx context.Context, claimed *model.Task, delay time.Duration) error {
	if _, err := e.repos.Tasks.RenewLease(ctx, claimed.ID, claimed.ClaimGeneration, e.config.LeaseDuration); err != nil {
		return err
	}
	deadline := time.Now().Add(delay)
	interval := minDuration(30*time.Second, e.config.LeaseDuration/3)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		wait := minDuration(interval, remaining)
		if !sleepContext(ctx, wait) {
			return ctx.Err()
		}
		if wait == remaining {
			return nil
		}
		if _, err := e.repos.Tasks.RenewLease(ctx, claimed.ID, claimed.ClaimGeneration, e.config.LeaseDuration); err != nil {
			return err
		}
	}
}

func invokeSettlement(ctx context.Context, settlement Settlement, repos *repository.Repositories) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("task settlement panicked: %v", recovered)
		}
	}()
	return settlement(ctx, repos)
}

func (e *Engine) transitionFor(claimed *model.Task, result Result) repository.TaskTransition {
	now := time.Now()
	transition := repository.TaskTransition{
		ResumeMode:    model.TaskResumeModeRecover,
		WaitReason:    textPointer(result.waitReason),
		FailureReason: textPointer(result.failureReason),
		LastError:     errorPointer(result.err),
		StatusMessage: textPointer(result.message),
	}
	switch result.kind {
	case resultComplete:
		retention := now.Add(e.config.Retention)
		transition.Status = model.TaskStatusCompleted
		transition.RetentionUntil = &retention
	case resultSuspend:
		delay := result.delay
		if result.resourceWait {
			delay = e.resourceWaitDelay(e.consecutiveResourceWaits(claimed.ID))
		}
		transition.Status = model.TaskStatusPending
		transition.ResumeMode = result.resumeMode
		transition.AvailableAt = now.Add(delay)
	case resultRetry:
		if claimed.RetryLimit != nil && claimed.RetryCount >= *claimed.RetryLimit {
			transition.Status = model.TaskStatusFailed
		} else {
			delay := result.delay
			if result.retryBackoff {
				delay = e.retryDelay(claimed.RetryCount)
			}
			transition.IncrementRetry = true
			transition.Status = model.TaskStatusPending
			transition.AvailableAt = now.Add(delay)
		}
	case resultFail:
		transition.Status = model.TaskStatusFailed
	case resultCancel:
		retention := now.Add(e.config.Retention)
		transition.Status = model.TaskStatusCancelled
		transition.RetentionUntil = &retention
	}
	return transition
}

func defaultRetryDelay(retryCount int) time.Duration {
	return backoffDelay(retryCount, retryBaseDelay, retryMaximumDelay, rand.Float64())
}

func defaultResourceWaitDelay(waits int) time.Duration {
	return backoffDelay(waits, resourceWaitBaseDelay, resourceWaitMaximumDelay, rand.Float64())
}

func backoffDelay(attempt int, base, maximum time.Duration, jitterUnit float64) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := base
	for range attempt {
		if delay >= maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	jitterUnit = min(max(jitterUnit, 0), 1)
	jitter := 1 + backoffJitterFraction*(2*jitterUnit-1)
	return min(time.Duration(float64(delay)*jitter), maximum)
}

func (e *Engine) consecutiveResourceWaits(id int64) int {
	e.resourceWaitMu.Lock()
	defer e.resourceWaitMu.Unlock()
	return e.resourceWaits[id]
}

// recordResourceWait extends a task's wait streak after a settled resource
// wait; any other settled result ends the streak.
func (e *Engine) recordResourceWait(id int64, waited bool) {
	e.resourceWaitMu.Lock()
	defer e.resourceWaitMu.Unlock()
	if waited {
		e.resourceWaits[id]++
		return
	}
	delete(e.resourceWaits, id)
}

func (e *Engine) renewLease(
	ctx context.Context,
	claimed *model.Task,
	leaseSafe *atomic.Bool,
	cancel context.CancelFunc,
	stop <-chan struct{},
	stopped chan<- struct{},
) {
	defer close(stopped)
	interval := minDuration(30*time.Second, e.config.LeaseDuration/3)
	confirmedUntil := time.Now().Add(e.config.LeaseDuration)
	if claimed.LeaseUntil != nil && claimed.LeaseUntil.After(time.Now()) {
		confirmedUntil = *claimed.LeaseUntil
	}
	safetyMargin := e.config.LeaseDuration / 3
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			leaseSafe.Store(false)
			return
		case <-stop:
			return
		case <-timer.C:
		}
		deadline := confirmedUntil.Add(-safetyMargin)
		var (
			err     error
			renewed time.Time
		)
		for attempt, delay := range e.renewalRetryDelays {
			if attempt > 0 {
				retry, stopped := waitRenewalRetry(ctx, stop, deadline, delay)
				if stopped {
					return
				}
				if !retry {
					leaseSafe.Store(false)
					cancel()
					return
				}
			}
			if !time.Now().Before(deadline) {
				err = context.DeadlineExceeded
				break
			}
			renewCtx, cancelRenew := context.WithDeadline(ctx, deadline)
			renewed, err = e.repos.Tasks.RenewLease(renewCtx, claimed.ID, claimed.ClaimGeneration, e.config.LeaseDuration)
			cancelRenew()
			if err == nil {
				confirmedUntil = renewed
				e.lastTick.Store(time.Now().UnixNano())
				break
			}
			if errors.Is(err, repository.ErrTaskLeaseLost) {
				break
			}
		}
		if err != nil {
			leaseSafe.Store(false)
			cancel()
			return
		}
		timer.Reset(interval)
	}
}

func waitRenewalRetry(ctx context.Context, stop <-chan struct{}, deadline time.Time, delay time.Duration) (retry, stopped bool) {
	remaining := time.Until(deadline)
	if remaining <= 0 || delay >= remaining {
		return false, false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, false
	case <-stop:
		return false, true
	case <-timer.C:
		return true, false
	}
}

func (e *Engine) abandonClaim(claimed *model.Task) {
	ctx, cancel := context.WithTimeout(context.Background(), minDuration(e.config.PollInterval, 5*time.Second))
	defer cancel()
	err := e.repos.Tasks.ShortenLease(ctx, claimed.ID, claimed.ClaimGeneration, e.config.PollInterval)
	if err == nil || errors.Is(err, repository.ErrTaskLeaseLost) {
		return
	}
	e.enqueueRecovery(recoveryRequest{id: claimed.ID, generation: claimed.ClaimGeneration})
}

func (e *Engine) runRecoveryQueue(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		request, ok := e.nextRecovery()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-e.recoveryWake:
				continue
			}
		}
		recoveryCtx, cancel := context.WithTimeout(context.Background(), minDuration(e.config.PollInterval, 5*time.Second))
		err := e.repos.Tasks.ShortenLease(recoveryCtx, request.id, request.generation, e.config.PollInterval)
		cancel()
		if err != nil && !errors.Is(err, repository.ErrTaskLeaseLost) {
			e.logger.Error("shortening uncertain task lease", "task_id", request.id, "claim_generation", request.generation, "error", err)
			if sleepContext(ctx, e.config.PollInterval) {
				e.enqueueRecovery(request)
			}
		}
	}
}

func (e *Engine) enqueueRecovery(request recoveryRequest) {
	e.recoveryMu.Lock()
	e.recovery[request] = struct{}{}
	e.recoveryMu.Unlock()
	select {
	case e.recoveryWake <- struct{}{}:
	default:
	}
}

func (e *Engine) nextRecovery() (recoveryRequest, bool) {
	e.recoveryMu.Lock()
	defer e.recoveryMu.Unlock()
	for request := range e.recovery {
		delete(e.recovery, request)
		return request, true
	}
	return recoveryRequest{}, false
}

// heldResource marks a context whose claim already holds a slot of a gate.
type heldResource struct{ resource Resource }

// withResource never waits for a slot: a full gate returns ErrResourceBusy so
// the worker is free to claim other tasks instead of idling behind the gate.
func (e *Engine) withResource(ctx context.Context, resource Resource, fn func(context.Context) error) error {
	gate, ok := e.gates[resource]
	if !ok || fn == nil {
		return fmt.Errorf("unknown task resource %q", resource)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Value(heldResource{resource}) != nil {
		return fn(ctx)
	}
	select {
	case gate <- struct{}{}:
	default:
		return ErrResourceBusy
	}
	defer func() { <-gate }()
	return fn(context.WithValue(ctx, heldResource{resource}, true))
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func textPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func errorPointer(err error) *string {
	if err == nil {
		return nil
	}
	message := err.Error()
	return &message
}

func decodeHash(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil
	}
	return decoded
}
