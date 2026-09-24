package backend

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type failingRangeSource struct {
	*bytes.Reader
	err error
}

func (s *failingRangeSource) Read(p []byte) (int, error) {
	n, err := s.Reader.Read(p)
	if errors.Is(err, io.EOF) {
		return 0, s.err
	}
	return n, err
}

func (*failingRangeSource) Close() error { return nil }

func TestRemoteRangeWaitsForSourceValidation(t *testing.T) {
	sourceErr := errors.New("piece checksum mismatch")
	body := newRangeReadCloser(&failingRangeSource{Reader: bytes.NewReader([]byte("abcdefgh")), err: sourceErr}, 2, 3, true)
	defer func() { _ = body.Close() }()
	buf := make([]byte, 3)
	n, err := body.Read(buf)
	if n != 0 || !errors.Is(err, sourceErr) {
		t.Fatalf("final range read = %d, %v; want withheld bytes and source error", n, err)
	}
}

type finalChunkErrorSource struct{ err error }

func (s *finalChunkErrorSource) Read(p []byte) (int, error) {
	copy(p, "abc")
	return 3, s.err
}

func (*finalChunkErrorSource) Close() error { return nil }

func TestRemoteRangeWithholdsFinalChunkReturnedWithError(t *testing.T) {
	sourceErr := errors.New("piece checksum mismatch")
	body := newRangeReadCloser(&finalChunkErrorSource{err: sourceErr}, 0, 3, true)
	defer func() { _ = body.Close() }()
	n, err := body.Read(make([]byte, 3))
	if n != 0 || !errors.Is(err, sourceErr) {
		t.Fatalf("final range read = %d, %v; want withheld bytes and source error", n, err)
	}
}

func TestCachedRangeStopsAtRequestedLength(t *testing.T) {
	body := newRangeReadCloser(io.NopCloser(bytes.NewReader([]byte("abcdefgh"))), 2, 3, false)
	defer func() { _ = body.Close() }()
	got, err := io.ReadAll(body)
	if err != nil || string(got) != "cde" {
		t.Fatalf("range body = %q, %v; want cde", got, err)
	}
}
