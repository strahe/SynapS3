// Package task owns the workflow-neutral task contract and execution engine.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

var (
	ErrEffectAlreadyAdmitted = repository.ErrTaskEffectAlreadyAdmitted
	ErrUnknownType           = errors.New("unknown task type")
	ErrInputConflict         = errors.New("task input conflict")
	ErrRetryUnsupported      = errors.New("task retry is not supported")
	ErrInvalidResult         = errors.New("invalid task result")
	ErrRegistryFrozen        = errors.New("task registry is frozen")
	ErrEffectForbidden       = errors.New("external effects are forbidden during recovery")
	ErrCodecPanic            = errors.New("task input codec panicked")
	ErrInvalidCanonical      = errors.New("task input codec returned invalid canonical JSON")
)

// Codec validates input and returns its canonical JSON representation.
type Codec interface {
	Canonicalize(json.RawMessage) (json.RawMessage, error)
}

// CodecFunc adapts a validation function to Codec.
type CodecFunc func(json.RawMessage) (json.RawMessage, error)

func (f CodecFunc) Canonicalize(input json.RawMessage) (json.RawMessage, error) {
	return f(input)
}

func canonicalizeInput(codec Codec, input json.RawMessage) (canonical json.RawMessage, err error) {
	if codec == nil {
		return nil, ErrInvalidCanonical
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			canonical = nil
			err = fmt.Errorf("%w: %v", ErrCodecPanic, recovered)
		}
	}()
	canonical, err = codec.Canonicalize(input)
	if err != nil {
		return nil, err
	}
	if !json.Valid(canonical) {
		return nil, ErrInvalidCanonical
	}
	return bytes.Clone(canonical), nil
}

// StrictJSONCodec rejects unknown fields and trailing values before applying
// validate. Re-marshalling produces stable object-key ordering.
func StrictJSONCodec[T any](validate func(*T) error) Codec {
	return CodecFunc(func(input json.RawMessage) (json.RawMessage, error) {
		var value T
		decoder := json.NewDecoder(bytes.NewReader(input))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("decoding task input: %w", err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, errors.New("decoding task input: trailing JSON value")
		}
		if validate != nil {
			if err := validate(&value); err != nil {
				return nil, err
			}
		}
		canonical, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encoding canonical task input: %w", err)
		}
		return canonical, nil
	})
}

// Definition is the runtime contract for a task type.
type Definition struct {
	Type model.TaskType
	// MaxConcurrency limits simultaneous executions of this type; zero uses only the global limit.
	MaxConcurrency int
	WorkStart      WorkStartPolicy
	InputVersion   int
	Codec          Codec
	Policy         ExecutionPolicy
	AllowRetry     bool
	// CanManualRetry optionally narrows AllowRetry for one failed task based on
	// its durable failure evidence. Nil preserves the type-wide policy.
	CanManualRetry func(*model.Task) bool
	// OnEngineFailure settles domain state when the engine fails a claim before
	// the handler can return its own result.
	OnEngineFailure func(*model.Task, string) Settlement
	// Identity derives the task's identity from its canonical input, so a
	// caller can never enqueue a round under another row's identity.
	Identity     IdentityFunc
	InspectRetry func(context.Context, *repository.Repositories, *model.Task) error
	PrepareRetry func(context.Context, *repository.Repositories, *model.Task) (RetryPreparation, error)
	// BindSuccessor moves domain ownership from a superseded round to its
	// successor in the same transaction, unless PrepareRetry overrides it.
	BindSuccessor func(context.Context, *repository.Repositories, *model.Task, *model.Task) error
	// RetryCheckpoint derives the successor evidence of a coordinator's budgeted
	// retry inside its transaction, so it must not call the network or wait on a
	// process lock. Nil inherits the failed round's checkpoint.
	RetryCheckpoint     func(context.Context, *repository.Repositories, *model.Task) (json.RawMessage, error)
	NextCycleCheckpoint func(context.Context, *repository.Repositories, *model.Task) (json.RawMessage, error)
}

type RetryPreparation struct {
	Request    EnqueueRequest
	Checkpoint json.RawMessage
	ResumeMode model.TaskResumeMode
	Validate   func(context.Context, *repository.Repositories, *model.Task) error
	Bind       func(context.Context, *repository.Repositories, *model.Task, *model.Task) error
	Release    func()
}

// WorkStartPolicy identifies when a task begins its own work.
type WorkStartPolicy uint8

const (
	// WorkStartOnHandler records work start before invoking the handler.
	WorkStartOnHandler WorkStartPolicy = iota + 1
	// WorkStartOnEffect leaves timing to WithCheckpointedEffect or validated
	// handler evidence saved with checkpoint or Result.WithWorkStartedAt settlement.
	WorkStartOnEffect
)

// Identity names the domain row a task works on and the round of work on it.
// Enqueue returns the current round of an identity instead of creating
// another; a nil Generation opts the task out of deduplication.
type Identity struct {
	SubjectType string
	SubjectKey  string
	Generation  *int64
}

// IdentityFunc derives a task's identity from its canonical input.
type IdentityFunc func(canonical json.RawMessage) (Identity, error)

// IdentityFromInput decodes the canonical input as T; identify returns the
// subject row ID, keyed by its decimal form, and the generation of work on it.
func IdentityFromInput[T any](subjectType string, identify func(T) (rowID, generation int64)) IdentityFunc {
	return func(canonical json.RawMessage) (Identity, error) {
		var value T
		if err := json.Unmarshal(canonical, &value); err != nil {
			return Identity{}, fmt.Errorf("decoding task identity: %w", err)
		}
		rowID, generation := identify(value)
		if rowID <= 0 || generation < 0 {
			return Identity{}, errors.New("task identity needs a positive row ID and a non-negative generation")
		}
		return Identity{SubjectType: subjectType, SubjectKey: strconv.FormatInt(rowID, 10), Generation: &generation}, nil
	}
}

// CycleInput is the input of a scheduled system task; Cycle numbers its round.
type CycleInput struct {
	Cycle int64 `json:"cycle"`
}

// CycleCodec validates scheduled system task input.
var CycleCodec = StrictJSONCodec(func(input *CycleInput) error {
	if input.Cycle < 1 {
		return errors.New("cycle must be positive")
	}
	return nil
})

// CycleIdentity identifies the cycles of one scheduled system task.
func CycleIdentity(subjectKey string) IdentityFunc {
	return func(canonical json.RawMessage) (Identity, error) {
		var input CycleInput
		if err := json.Unmarshal(canonical, &input); err != nil {
			return Identity{}, fmt.Errorf("decoding task identity: %w", err)
		}
		return Identity{SubjectType: "system", SubjectKey: subjectKey, Generation: &input.Cycle}, nil
	}
}

func (d Definition) manualRetryAllowed(task *model.Task) bool {
	return d.AllowRetry && (d.CanManualRetry == nil || d.CanManualRetry(task))
}

func (d Definition) validate() error {
	if d.Type == "" || d.InputVersion < 1 || d.Codec == nil || d.Identity == nil || d.MaxConcurrency < 0 {
		return fmt.Errorf("incomplete definition for %q", d.Type)
	}
	if err := d.Policy.validate(); err != nil {
		return fmt.Errorf("policy for %q: %w", d.Type, err)
	}
	if d.WorkStart != WorkStartOnHandler && d.WorkStart != WorkStartOnEffect {
		return fmt.Errorf("work start policy is required for %q", d.Type)
	}
	return nil
}

// Handler owns exactly one task type.
type Handler interface {
	Definition() Definition
	Execute(context.Context, Execution) Result
	Recover(context.Context, Execution) Result
}

type ResultKind uint8

const (
	resultInvalid ResultKind = iota
	resultComplete
	resultWait
	resultRetry
	resultFail
	resultCancel
)

// Settlement performs database-only domain changes in the task transition
// transaction. It must be safe to invoke again after a rollback.
type Settlement func(context.Context, *repository.Repositories) error

// Result is a closed set constructed through the helpers below.
type Result struct {
	kind                 ResultKind
	waitUntil            *time.Time
	observationWait      bool
	observationWindow    *observationWindow
	resumeMode           model.TaskResumeMode
	waitReason           string
	failureReason        string
	err                  error
	diagnostic           error
	message              string
	settlement           Settlement
	contentionSettlement Settlement
	onRetry              RetrySettlement
	onExhausted          Settlement
	workStartedAt        *time.Time
}

type observationWindow struct {
	stageKey         string
	timeoutErr       error
	timeoutRetryMode model.TaskResumeMode
}

// WithDiagnostic preserves an observation failure without consuming a retry.
// Only Wait results may carry a diagnostic.
func (r Result) WithDiagnostic(err error) Result {
	r.diagnostic = err
	return r
}

// WithContentionSettlement retains only recovery evidence when database
// contention defers the business result. It must not complete or fail the
// operation or authorize another external effect.
func (r Result) WithContentionSettlement(settlement Settlement) Result {
	r.contentionSettlement = settlement
	return r
}

// WithObservationWindow lets the engine retry an observation stage when its
// policy window expires. The same stage retains its start across claims.
func (r Result) WithObservationWindow(stageKey string, timeoutErr error, timeoutRetryMode model.TaskResumeMode) Result {
	r.observationWindow = &observationWindow{stageKey: stageKey, timeoutErr: timeoutErr, timeoutRetryMode: timeoutRetryMode}
	return r
}

// WithWorkStartedAt carries observed operation timing into fenced settlement.
// Zero means no operation was observed; existing first-start evidence is kept.
func (r Result) WithWorkStartedAt(startedAt time.Time) Result {
	if !startedAt.IsZero() {
		startedAt = startedAt.UTC()
		r.workStartedAt = &startedAt
	}
	return r
}

func Complete(message string, settlement Settlement) Result {
	return Result{kind: resultComplete, message: message, settlement: settlement}
}

func Wait(mode model.TaskResumeMode, reason, message string, settlement Settlement) Result {
	return Result{
		kind: resultWait, resumeMode: mode,
		waitReason: reason, message: message, settlement: settlement,
	}
}

func WaitForObservation(mode model.TaskResumeMode, reason, message string, settlement Settlement) Result {
	r := Wait(mode, reason, message, settlement)
	r.observationWait = true
	return r
}

func WaitUntil(mode model.TaskResumeMode, until time.Time, reason, message string, settlement Settlement) Result {
	r := Wait(mode, reason, message, settlement)
	until = until.UTC()
	r.waitUntil = &until
	return r
}

func Retry(err error, failureReason string, settlement Settlement) Result {
	return Result{
		kind: resultRetry, resumeMode: model.TaskResumeModeRecover,
		failureReason: failureReason, err: err, settlement: settlement,
	}
}

func Fail(err error, failureReason string, settlement Settlement) Result {
	return Result{kind: resultFail, err: err, failureReason: failureReason, settlement: settlement}
}

func Cancel(message string, settlement Settlement) Result {
	return Result{kind: resultCancel, message: message, settlement: settlement}
}

type checkpointWriter func(ctx context.Context, value any, settlement Settlement, startWork, obtainedEvidence bool) error

// effectEvidenceTimeout bounds a write that outlives its caller's cancellation.
const effectEvidenceTimeout = 10 * time.Second

// Execution is an immutable view of one fenced claim.
type Execution struct {
	task       model.Task
	policy     ExecutionPolicy
	checkpoint checkpointWriter
	admit      func(context.Context, string, any, Settlement) error
	observe    func(context.Context, string) (time.Time, error)
	resolve    func(context.Context, string, any, Settlement) error
}

func (e Execution) ID() int64                   { return e.task.ID }
func (e Execution) Type() model.TaskType        { return e.task.Type }
func (e Execution) InputVersion() int           { return e.task.InputVersion }
func (e Execution) ClaimGeneration() int64      { return e.task.ClaimGeneration }
func (e Execution) Mode() model.TaskResumeMode  { return e.task.ResumeMode }
func (e Execution) RetryCount() int             { return e.task.RetryCount }
func (e Execution) LastError() string           { return dereference(e.task.LastError) }
func (e Execution) Input() json.RawMessage      { return bytes.Clone(e.task.Input) }
func (e Execution) Checkpoint() json.RawMessage { return bytes.Clone(e.task.Checkpoint) }

func (e Execution) WriteCheckpoint(ctx context.Context, value any) error {
	return e.WriteCheckpointWith(ctx, value, nil)
}

// WriteCheckpointWith atomically persists recovery identity and monotonic
// domain evidence before an external effect can be attempted.
func (e Execution) WriteCheckpointWith(ctx context.Context, value any, settlement Settlement) error {
	if e.checkpoint == nil {
		return errors.New("checkpoint writer is unavailable")
	}
	return e.checkpoint(ctx, value, settlement, false, false)
}

// RecordEffectEvidence persists evidence an external effect already returned.
// It survives caller cancellation so shutdown cannot drop an obtained
// credential; the claim fence is still checked in the same transaction.
func (e Execution) RecordEffectEvidence(ctx context.Context, value any, settlement Settlement) error {
	if e.checkpoint == nil {
		return errors.New("checkpoint writer is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), effectEvidenceTimeout)
	defer cancel()
	return e.checkpoint(ctx, value, settlement, false, true)
}

// WithCheckpointedEffect admits an external effect, persists its recovery
// evidence, and only then invokes effect. A false attempted result guarantees
// that effect was not called; recovery after a process crash must still rely on
// the durable checkpoint rather than this return value.
func (e Execution) WithCheckpointedEffect(
	ctx context.Context,
	operationKey string,
	checkpoint any,
	settlement Settlement,
	effect func(context.Context) error,
) (attempted bool, err error) {
	if effect == nil {
		return false, errors.New("external effect is required")
	}
	if e.Mode() != model.TaskResumeModeExecute {
		return false, ErrEffectForbidden
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if e.admit == nil {
		return false, errors.New("effect admission is unavailable")
	}
	if err := e.admit(ctx, operationKey, checkpoint, settlement); err != nil {
		return false, err
	}
	return true, effect(ctx)
}

func DecodeInput[T any](execution Execution) (T, error) {
	var value T
	err := json.Unmarshal(execution.Input(), &value)
	return value, err
}

func DecodeCheckpoint[T any](execution Execution) (T, bool, error) {
	var value T
	raw := execution.Checkpoint()
	if len(raw) == 0 {
		return value, false, nil
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return value, true, err
	}
	return value, true, nil
}

func dereference(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// RetrySettlement receives the next opportunity's earliest execution time.
type RetrySettlement func(context.Context, *repository.Repositories, time.Time) error

func (r Result) WithRetrySettlements(onRetry RetrySettlement, onExhausted Settlement) Result {
	r.onRetry, r.onExhausted = onRetry, onExhausted
	return r
}

func (r Result) WithResumeMode(mode model.TaskResumeMode) Result { r.resumeMode = mode; return r }
func RetryInMode(err error, reason string, mode model.TaskResumeMode, settlement Settlement) Result {
	return Retry(err, reason, settlement).WithResumeMode(mode)
}

func CompleteCycle(message string, settlement Settlement) Result {
	return Complete(message, settlement)
}

func (e Execution) Policy() ExecutionPolicy { return e.policy.clone() }

func (e Execution) ResendAt(sentAt time.Time, sends int) (time.Time, error) {
	p := e.policy.ResendBackoff
	if sentAt.IsZero() || sends < 1 || p == nil || !p.valid() || p.Jitter != 0 {
		return time.Time{}, ErrInvalidPolicy
	}
	return sentAt.UTC().Add(time.Duration(p.baseDelay(sends - 1))), nil
}

func (e Execution) DependencyRetryAt(retries int) (time.Time, bool) {
	if retries < 0 || retries >= e.policy.DependencyRetries {
		return time.Time{}, false
	}
	return time.Now().UTC().Add(e.policy.Backoff.delay(retries)), true
}

func (e Execution) ObserveOperation(ctx context.Context, key string) (time.Time, error) {
	if e.observe == nil {
		return time.Time{}, errors.New("operation observation is unavailable")
	}
	return e.observe(ctx, key)
}

func (e Execution) ResolveOperation(ctx context.Context, key string, checkpoint any, settlement Settlement) error {
	if e.resolve == nil {
		return errors.New("operation resolution is unavailable")
	}
	return e.resolve(ctx, key, checkpoint, settlement)
}
