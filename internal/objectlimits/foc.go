package objectlimits

import (
	"errors"
	"fmt"
	"io"

	"github.com/strahe/synapse-go/chain"
)

const (
	MinFOCUploadSize = int64(chain.MinUploadSize)
	MaxFOCUploadSize = int64(chain.MaxUploadSize)
)

var (
	ErrTooSmall = errors.New("object size below FOC minimum")
	ErrTooLarge = errors.New("object size above FOC maximum")
)

type SizeError struct {
	Size int64
	Err  error
}

func (e *SizeError) Error() string {
	switch {
	case errors.Is(e.Err, ErrTooSmall):
		return fmt.Sprintf("object size %d is below FOC minimum %d bytes", e.Size, MinFOCUploadSize)
	case errors.Is(e.Err, ErrTooLarge):
		return fmt.Sprintf("object size %d exceeds FOC maximum %d bytes", e.Size, MaxFOCUploadSize)
	default:
		return "object size violates FOC upload limits"
	}
}

func (e *SizeError) Unwrap() error {
	return e.Err
}

func ValidateFOCUploadSize(size int64) error {
	switch {
	case size < MinFOCUploadSize:
		return &SizeError{Size: size, Err: ErrTooSmall}
	case size > MaxFOCUploadSize:
		return &SizeError{Size: size, Err: ErrTooLarge}
	default:
		return nil
	}
}

func LimitFOCUploadReader(r io.Reader) io.Reader {
	return &maxReader{r: r, remaining: MaxFOCUploadSize}
}

type maxReader struct {
	r         io.Reader
	remaining int64
}

func (r *maxReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.r.Read(probe[:])
		if n > 0 {
			return 0, &SizeError{Size: MaxFOCUploadSize + 1, Err: ErrTooLarge}
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	return n, err
}
