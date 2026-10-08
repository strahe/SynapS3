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
	ErrResourceBusy          = errors.New("task resource is busy")
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

// Definition is the complete persistent contract for a task type.
type Definition struct {
	Type         model.TaskType
	WorkStart    WorkStartPolicy
	InputVersion int
	Codec        Codec
	Policy       ExecutionPolicy
	AllowRetry   bool
	// CanManualRetry optionally narrows AllowRetry for one failed task based on
	// its durable failure evidence. Nil preserves the type-wide policy.
	CanManualRetry func(*model.Task) bool
	// OnEngineFailure settles domain state when the engine fails a claim before
	// the handler can return its own result.
	OnEngineFailure func(*model.Task, string) Settlement
	// Subject derives the task's subject from its canonical input. Types whose
	// tasks repository logic finds by subject declare it; enqueue then fills
	// the subject and refuses one that names a different row.
	Subject             SubjectFunc
	InspectRetry        func(context.Context, *repository.Repositories, *model.Task) error
	PrepareRetry        func(context.Context, *repository.Repositories, *model.Task) (RetryPreparation, error)
	LegacyHandoff       func(context.Context, *repository.Repositories, *model.Task, *model.Task) error
	NextCycleCheckpoint func(*model.Task) json.RawMessage
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

// Subject names the domain row a task works on.
type Subject struct {
	Type string
	Key  string
}

// SubjectFunc derives a task's subject from its canonical input.
type SubjectFunc func(canonical json.RawMessage) (Subject, error)

// SubjectFromInput decodes the canonical input as T and names the row id
// returns, keyed by its decimal ID.
func SubjectFromInput[T any](subjectType string, id func(T) int64) SubjectFunc {
	return func(canonical json.RawMessage) (Subject, error) {
		var value T
		if err := json.Unmarshal(canonical, &value); err != nil {
			return Subject{}, fmt.Errorf("decoding task subject: %w", err)
		}
		rowID := id(value)
		if rowID <= 0 {
			return Subject{}, errors.New("task subject ID must be positive")
		}
		return Subject{Type: subjectType, Key: strconv.FormatInt(rowID, 10)}, nil
	}
}

func (d Definition) manualRetryAllowed(task *model.Task) bool {
	return d.AllowRetry && (d.CanManualRetry == nil || d.CanManualRetry(task))
}

func (d Definition) validate() error {
	if d.Type == "" || d.InputVersion < 1 || d.Codec == nil {
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
	kind          ResultKind
	delay         time.Duration
	retryBackoff  bool
	resourceWait  bool
	resumeMode    model.TaskResumeMode
	waitReason    string
	failureReason string
	err           error
	message       string
	settlement    Settlement
	onRetry       RetrySettlement
	onExhausted   Settlement
	cycleDelay    time.Duration
	workStartedAt *time.Time
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

func Wait(mode model.TaskResumeMode, delay time.Duration, reason, message string, settlement Settlement) Result {
	return Result{
		kind: resultWait, resumeMode: mode, delay: delay,
		waitReason: reason, message: message, settlement: settlement,
	}
}

// ResourceWait yields a task that found its resource gate full. It resumes in
// execute mode after a backoff that grows with the task's consecutive waits,
// and it consumes no retry budget.
func ResourceWait(message string) Result {
	return Result{
		kind: resultWait, resumeMode: model.TaskResumeModeExecute, resourceWait: true,
		waitReason: "resource", message: message,
	}
}

func Retry(err error, failureReason string, delay time.Duration, settlement Settlement) Result {
	return Result{
		kind: resultRetry, resumeMode: model.TaskResumeModeRecover, delay: delay, retryBackoff: true,
		failureReason: failureReason, err: err, settlement: settlement,
	}
}

// RetryBackoff retries with the engine's bounded exponential backoff.
func RetryBackoff(err error, failureReason string, settlement Settlement) Result {
	return Result{
		kind: resultRetry, resumeMode: model.TaskResumeModeRecover, retryBackoff: true,
		failureReason: failureReason, err: err, settlement: settlement,
	}
}

func Fail(err error, failureReason string, settlement Settlement) Result {
	return Result{kind: resultFail, err: err, failureReason: failureReason, settlement: settlement}
}

func Cancel(message string, settlement Settlement) Result {
	return Result{kind: resultCancel, message: message, settlement: settlement}
}

type Resource string

const (
	ResourceProviderMutation    Resource = "provider_mutation"
	ResourceProviderUploadSpeed Resource = "provider_upload_speed"
	ResourceDestructiveMutation Resource = "destructive_mutation"
	ResourceWallet              Resource = "wallet"
)

type (
	checkpointWriter func(context.Context, any, Settlement, bool) error
	resourceRunner   func(context.Context, Resource, func(context.Context) error) error
)

// Execution is an immutable view of one fenced claim.
type Execution struct {
	task       model.Task
	checkpoint checkpointWriter
	resource   resourceRunner
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
func (e Execution) CancellationRequested() bool { return e.task.CancellationRequested() }
func (e Execution) CancellationReason() string  { return dereference(e.task.CancellationReason) }
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
	return e.checkpoint(ctx, value, settlement, false)
}

// WithResource runs fn while holding one slot of resource. It never waits for a
// slot: a full gate returns ErrResourceBusy, which handlers turn into
// ResourceWait. A nested call for a resource the context already holds reuses
// that slot.
func (e Execution) WithResource(ctx context.Context, resource Resource, fn func(context.Context) error) error {
	if e.Mode() != model.TaskResumeModeExecute {
		return ErrEffectForbidden
	}
	if e.resource == nil {
		return errors.New("resource gate is unavailable")
	}
	return e.resource(ctx, resource, fn)
}

// WithCheckpointedEffect admits an external effect, persists its recovery
// evidence, and only then invokes effect. A false attempted result guarantees
// that effect was not called; recovery after a process crash must still rely on
// the durable checkpoint rather than this return value.
func (e Execution) WithCheckpointedEffect(
	ctx context.Context,
	resource Resource,
	operationKey string,
	checkpoint any,
	settlement Settlement,
	effect func(context.Context) error,
) (attempted bool, err error) {
	if effect == nil {
		return false, errors.New("external effect is required")
	}
	err = e.WithResource(ctx, resource, func(ctx context.Context) error {
		if e.admit == nil {
			return errors.New("effect admission is unavailable")
		}
		if err := e.admit(ctx, operationKey, checkpoint, settlement); err != nil {
			return err
		}
		attempted = true
		return effect(ctx)
	})
	return attempted, err
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
func (r Result) WithMinimumDelay(delay time.Duration) Result {
	r.delay = delay
	r.retryBackoff = true
	return r
}

func RetryInMode(err error, reason string, mode model.TaskResumeMode, minimumDelay time.Duration, settlement Settlement) Result {
	return RetryBackoff(err, reason, settlement).WithResumeMode(mode).WithMinimumDelay(minimumDelay)
}

func CompleteCycle(delay time.Duration, message string, settlement Settlement) Result {
	r := Complete(message, settlement)
	r.cycleDelay = delay
	return r
}

func (e Execution) Policy() ExecutionPolicy { p, _ := DecodePolicy(&e.task); return p }
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
