package worker

import (
	"context"
	"errors"
)

// FuncHandler adapts functions to one registered task type.
type FuncHandler struct {
	definition Definition
	execute    func(context.Context, Execution) Result
	recover    func(context.Context, Execution) Result
}

func NewFuncHandler(definition Definition, execute, recover func(context.Context, Execution) Result) *FuncHandler {
	return &FuncHandler{definition: definition, execute: execute, recover: recover}
}

func (h *FuncHandler) Definition() Definition { return h.definition }

func (h *FuncHandler) Execute(ctx context.Context, execution Execution) Result {
	if h.execute == nil {
		return Fail(errors.New("execute handler is unavailable"), "handler_unavailable", nil)
	}
	return h.execute(ctx, execution)
}

func (h *FuncHandler) Recover(ctx context.Context, execution Execution) Result {
	if h.recover == nil {
		return Fail(errors.New("recover handler is unavailable"), "handler_unavailable", nil)
	}
	return h.recover(ctx, execution)
}
