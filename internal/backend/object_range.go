package backend

import (
	"errors"
	"io"
)

// rangeReadCloser keeps the original stream open until the requested response
// finishes. A remote stream must reach EOF to validate its piece CID and finish
// the cache rehydration; its final response bytes are held until that succeeds.
type rangeReadCloser struct {
	source    io.ReadCloser
	skip      int64
	remaining int64
	validate  bool
	finished  bool
}

func newRangeReadCloser(source io.ReadCloser, start, length int64, validate bool) io.ReadCloser {
	return &rangeReadCloser{source: source, skip: start, remaining: length, validate: validate}
}

func (r *rangeReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.finished {
		return 0, io.EOF
	}
	if r.skip > 0 {
		if _, err := io.CopyN(io.Discard, r.source, r.skip); err != nil {
			return 0, err
		}
		r.skip = 0
	}
	if int64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, err := r.source.Read(p)
	r.remaining -= int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		if r.validate && r.remaining == 0 {
			return 0, err
		}
		return n, err
	}
	if r.remaining > 0 {
		if errors.Is(err, io.EOF) {
			return n, io.ErrUnexpectedEOF
		}
		return n, nil
	}
	if r.validate {
		var discard [32 * 1024]byte
		noProgress := 0
		for {
			count, drainErr := r.source.Read(discard[:])
			if errors.Is(drainErr, io.EOF) {
				break
			}
			if drainErr != nil {
				return 0, drainErr
			}
			if count == 0 {
				noProgress++
				if noProgress >= 100 {
					return 0, io.ErrNoProgress
				}
			} else {
				noProgress = 0
			}
		}
	}
	r.finished = true
	return n, nil
}

func (r *rangeReadCloser) Close() error {
	return r.source.Close()
}
