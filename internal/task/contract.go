// Package task owns the workflow-neutral task contract and execution engine.
package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

var (
	ErrUnknownType      = errors.New("unknown task type")
	ErrInputConflict    = errors.New("task input conflict")
	ErrRetryUnsupported = errors.New("task retry is not supported")
	ErrInvalidResult    = errors.New("invalid task result")
	ErrRegistryFrozen   = errors.New("task registry is frozen")
	ErrEffectForbidden  = errors.New("external effects are forbidden during recovery")
	ErrCodecPanic       = errors.New("task input codec panicked")
	ErrInvalidCanonical = errors.New("task input codec returned invalid canonical JSON")
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
	InputVersion int
	Codec        Codec
	RetryLimit   *int
	AllowRetry   bool
	// CanManualRetry optionally narrows AllowRetry for one failed task based on
	// its durable failure evidence. Nil preserves the type-wide policy.
	CanManualRetry func(*model.Task) bool
}

func (d Definition) manualRetryAllowed(task *model.Task) bool {
	return d.AllowRetry && (d.CanManualRetry == nil || d.CanManualRetry(task))
}

func (d Definition) validate() error {
	if d.Type == "" || d.InputVersion < 1 || d.Codec == nil {
		return fmt.Errorf("incomplete definition for %q", d.Type)
	}
	if d.RetryLimit != nil && *d.RetryLimit < 0 {
		return fmt.Errorf("negative retry limit for %q", d.Type)
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
	resultSuspend
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
	resumeMode    model.TaskResumeMode
	waitReason    string
	failureReason string
	err           error
	message       string
	settlement    Settlement
}

func Complete(message string, settlement Settlement) Result {
	return Result{kind: resultComplete, message: message, settlement: settlement}
}

func Suspend(mode model.TaskResumeMode, delay time.Duration, reason, message string, settlement Settlement) Result {
	return Result{
		kind: resultSuspend, resumeMode: mode, delay: delay,
		waitReason: reason, message: message, settlement: settlement,
	}
}

func Retry(err error, failureReason string, delay time.Duration, settlement Settlement) Result {
	return Result{
		kind: resultRetry, resumeMode: model.TaskResumeModeRecover, delay: delay,
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
	ResourceDestructiveMutation Resource = "destructive_mutation"
	ResourceWallet              Resource = "wallet"
)

type (
	checkpointWriter func(context.Context, any, Settlement) error
	resourceRunner   func(context.Context, Resource, func(context.Context) error) error
)

// Execution is an immutable view of one fenced claim.
type Execution struct {
	task       model.Task
	checkpoint checkpointWriter
	resource   resourceRunner
}

func (e Execution) ID() int64                  { return e.task.ID }
func (e Execution) Type() model.TaskType       { return e.task.Type }
func (e Execution) InputVersion() int          { return e.task.InputVersion }
func (e Execution) ClaimGeneration() int64     { return e.task.ClaimGeneration }
func (e Execution) Mode() model.TaskResumeMode { return e.task.ResumeMode }
func (e Execution) RetryCount() int            { return e.task.RetryCount }
func (e Execution) RetryLimit() (int, bool) {
	if e.task.RetryLimit == nil {
		return 0, false
	}
	return *e.task.RetryLimit, true
}

func (e Execution) RetryWillFail() bool {
	limit, limited := e.RetryLimit()
	return limited && e.RetryCount() >= limit
}
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
	return e.checkpoint(ctx, value, settlement)
}

func (e Execution) WithResource(ctx context.Context, resource Resource, fn func(context.Context) error) error {
	if e.Mode() != model.TaskResumeModeExecute {
		return ErrEffectForbidden
	}
	if e.resource == nil {
		return errors.New("resource gate is unavailable")
	}
	return e.resource(ctx, resource, fn)
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
