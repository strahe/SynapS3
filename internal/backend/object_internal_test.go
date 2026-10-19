package backend

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/versity/versitygw/s3err"
)

func TestContentWriteErrorPreservesS3Errors(t *testing.T) {
	for _, want := range []s3err.S3Error{
		s3err.GetAPIError(s3err.ErrIncompleteBody),
		s3err.GetInvalidArgumentErr(s3err.InvalidArgPartNumber, "0"),
	} {
		t.Run(want.BaseError().Code, func(t *testing.T) {
			got := contentWriteError(fmt.Errorf("staging object: %w", fmt.Errorf("reading body: %w", want)))
			if got != want {
				t.Fatalf("contentWriteError = %T %v, want original %T %v", got, got, want, want)
			}
		})
	}
}

func TestContentWriteErrorKeepsStorageMappings(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want s3err.ErrorCode
	}{
		{name: "cleanup", err: repository.ErrContentCleanupInProgress, want: s3err.ErrSlowDown},
		{name: "cache full", err: cache.ErrCacheFull, want: s3err.ErrSlowDown},
		{name: "size exceeded", err: cache.ErrSizeExceeded, want: s3err.ErrContentLengthMismatch},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.Join(s3err.GetAPIError(s3err.ErrIncompleteBody), fmt.Errorf("cache write: %w", tt.err))
			got, ok := contentWriteError(err).(s3err.S3Error)
			want := s3err.GetAPIError(tt.want)
			if !ok || got.BaseError().Code != want.Code || got.StatusCode() != want.StatusCode() {
				t.Fatalf("contentWriteError = %v, want %s (%d)", got, want.Code, want.StatusCode())
			}
		})
	}
	for _, err := range []error{
		errors.New("disk write failed"),
		fmt.Errorf("reading copy source: %w", io.ErrUnexpectedEOF),
	} {
		if got := contentWriteError(err); got != err {
			t.Fatalf("contentWriteError = %v, want original error %v", got, err)
		}
	}
}
