package backend

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/valyala/fasthttp"
)

func TestRequestContextPreservesValuesAndShutdownCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	contexts := make(chan context.Context, 1)
	server := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.SetUserValue("request-marker", "preserved")
		contexts <- snapshotRequestContext(ctx)
		ctx.SetUserValue("request-marker", "changed")
	}}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Shutdown() })

	response, err := http.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	ctx := <-contexts
	if ctx.Value("request-marker") != "preserved" || ctx.Value([]byte("request-marker")) != "preserved" {
		t.Errorf("request values changed after the handler returned")
	}
	if ctx.Err() != nil {
		t.Errorf("context canceled before server shutdown: %v", ctx.Err())
	}
	if err := server.ShutdownWithContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Error("context lost its shutdown cancellation signal")
	}
	if ctx.Err() != context.Canceled {
		t.Errorf("context error after server shutdown = %v, want canceled", ctx.Err())
	}
}

func TestRequestContextPreservesStandardContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if got := snapshotRequestContext(ctx); got != ctx {
		t.Fatal("standard context was replaced")
	}
}
