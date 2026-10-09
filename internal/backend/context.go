package backend

import (
	"context"
	"time"

	"github.com/valyala/fasthttp"
)

// snapshotRequestContext copies values out of fasthttp's pooled RequestCtx and
// retains its server shutdown signal after the handler returns.
func snapshotRequestContext(ctx context.Context) context.Context {
	requestCtx, ok := ctx.(*fasthttp.RequestCtx)
	if !ok {
		return ctx
	}
	values := make(map[any]any)
	requestCtx.VisitUserValuesAll(func(key, value any) { values[key] = value })
	return requestContext{done: requestCtx.Done(), values: values}
}

type requestContext struct {
	done   <-chan struct{}
	values map[any]any
}

func (ctx requestContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (ctx requestContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx requestContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func (ctx requestContext) Value(key any) any {
	if bytes, ok := key.([]byte); ok {
		key = string(bytes)
	}
	return ctx.values[key]
}
