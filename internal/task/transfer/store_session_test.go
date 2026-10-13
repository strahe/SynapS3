package transfer

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synapse-go/piece"
)

type storeReaderCloseRecorder struct {
	io.Reader
	closeErr error
	closes   atomic.Int64
}

func (r *storeReaderCloseRecorder) Close() error {
	r.closes.Add(1)
	return r.closeErr
}

func TestStoreContextReaderCloseIsIdempotent(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "complete"
		if canceled {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			closeErr := errors.New("cache close failed")
			raw := &storeReaderCloseRecorder{Reader: strings.NewReader("cached bytes"), closeErr: closeErr}
			reader := newStoreContextReader(ctx, raw)
			if canceled {
				cancel()
			}
			closed := make(chan error, 1)
			go func() {
				if err := reader.Close(); !errors.Is(err, closeErr) {
					closed <- err
					return
				}
				closed <- reader.Close()
			}()
			select {
			case err := <-closed:
				if !errors.Is(err, closeErr) || raw.closes.Load() != 1 {
					t.Fatalf("repeated Close: calls=%d err=%v", raw.closes.Load(), err)
				}
			case <-time.After(time.Second):
				t.Fatal("repeated Close did not return")
			}
		})
	}
}

type storeHashPipeReader struct {
	*io.PipeReader
	started chan struct{}
	once    sync.Once
}

func (r *storeHashPipeReader) Read(data []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	return r.PipeReader.Read(data)
}

func TestStoreContextReaderCancellationStopsPieceCalculation(t *testing.T) {
	pipeReader, pipeWriter := io.Pipe()
	defer func() { _ = pipeReader.Close() }()
	defer func() { _ = pipeWriter.Close() }()
	raw := &storeHashPipeReader{PipeReader: pipeReader, started: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader := newStoreContextReader(ctx, raw)
	calculated := make(chan error, 1)
	go func() {
		_, err := piece.Calculate(reader)
		calculated <- err
	}()
	select {
	case <-raw.started:
	case <-time.After(time.Second):
		t.Fatal("piece calculation did not start reading cached bytes")
	}
	cancel()
	select {
	case err := <-calculated:
		if err == nil {
			t.Fatal("canceled piece calculation returned success")
		}
	case <-time.After(time.Second):
		t.Fatal("piece calculation did not stop after cancellation")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close after canceled calculation: %v", err)
	}
}

type storeUnwindingReader struct {
	*io.PipeReader
	started    chan struct{}
	unwinding  chan struct{}
	finishRead chan struct{}
	exited     atomic.Bool
}

func (r *storeUnwindingReader) Read(data []byte) (int, error) {
	close(r.started)
	n, err := r.PipeReader.Read(data)
	close(r.unwinding)
	<-r.finishRead
	r.exited.Store(true)
	return n, err
}

func TestStoreCancellationRetainsCacheLockUntilReadExits(t *testing.T) {
	pipeReader, pipeWriter := io.Pipe()
	defer func() { _ = pipeWriter.Close() }()
	raw := &storeUnwindingReader{
		PipeReader: pipeReader, started: make(chan struct{}),
		unwinding: make(chan struct{}), finishRead: make(chan struct{}),
	}
	defer func() {
		select {
		case <-raw.finishRead:
		default:
			close(raw.finishRead)
		}
		_ = pipeReader.Close()
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gate := cacheaccess.NewGate()
	release, err := gate.HoldRead(t.Context(), "store-body")
	if err != nil {
		t.Fatal(err)
	}
	body := &storeBody{storeContextReader: newStoreContextReader(ctx, raw), release: release}
	readDone := make(chan error, 1)
	go func() {
		_, err := body.Read(make([]byte, 1))
		readDone <- err
	}()
	select {
	case <-raw.started:
	case <-time.After(time.Second):
		t.Fatal("upload did not start reading cached bytes")
	}
	cancel()
	select {
	case <-raw.unwinding:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close the underlying cache reader")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- body.Close() }()
	deleteDone := make(chan error, 1)
	deleteCtx, cancelDelete := context.WithTimeout(t.Context(), time.Second)
	defer cancelDelete()
	go func() {
		deleteDone <- gate.GuardDeletion(deleteCtx, "store-body", func() {
			if !raw.exited.Load() {
				t.Error("cache deletion began while the upload reader was still running")
			}
		})
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("cache lock released before the upload reader exited: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(raw.finishRead)
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("canceled upload read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("upload read did not exit")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close canceled body: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("upload body did not close after the underlying read exited")
	}
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("cache lock was not released after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cache deletion remained blocked after the upload reader exited")
	}
}
