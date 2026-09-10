package bucketlifecycle

import (
	"fmt"

	"github.com/strahe/synaps3/internal/db/repository"
)

// ProvisionInput identifies the bucket whose provider storage must be ready.
type ProvisionInput struct {
	BucketID int64 `json:"bucket_id"`
}

func ValidateProvisionInput(input ProvisionInput) error {
	if input.BucketID < 1 {
		return fmt.Errorf("bucketID must be positive: %w", repository.ErrInvalidInput)
	}
	return nil
}

// ProvisionKey scopes provisioning to a replica target, so raising a bucket's
// target schedules a fresh run instead of colliding with the completed one that
// provisioned the smaller target.
//
// This is only sound while the target can never go down: a target that returned
// to an earlier value would rebuild a key whose task already completed, and the
// slots opened for it would never be provisioned. Whoever implements lowering
// has to replace this with a monotonic generation first.
func ProvisionKey(bucketID int64, copies int) string {
	return fmt.Sprintf("bucket:%d:provision:%d", bucketID, copies)
}
