package bucketlifecycle

import (
	"fmt"

	"github.com/strahe/synaps3/internal/db/repository"
)

// ProvisionInput identifies the bucket whose provider storage must be ready
// for a replica target. The target is the task's generation, so raising it
// schedules a fresh run instead of colliding with the completed one that
// provisioned the smaller target.
//
// This is only sound while the target can never go down: a target that returned
// to an earlier value would name a round that already completed, and the slots
// opened for it would never be provisioned. Whoever implements lowering has to
// replace it with a monotonic generation first.
type ProvisionInput struct {
	BucketID int64 `json:"bucket_id"`
	Copies   int   `json:"copies"`
}

func ValidateProvisionInput(input ProvisionInput) error {
	if input.BucketID < 1 || input.Copies < 1 {
		return fmt.Errorf("bucketID and copies must be positive: %w", repository.ErrInvalidInput)
	}
	return nil
}
