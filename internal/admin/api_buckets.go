package admin

import (
	"net/http"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
)

type bucketListItem struct {
	ID             int64   `json:"id"`
	Name           string  `json:"name"`
	Status         string  `json:"status"`
	ProofSetID     *string `json:"proof_set_id"`
	ObjectCount    int64   `json:"object_count"`
	TotalSizeBytes int64   `json:"total_size_bytes"`
	CreatedAt      string  `json:"created_at"`
}

func (s *Server) handleAPIBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	buckets, err := s.repos.Buckets.ListActive(ctx)
	if err != nil {
		s.logger.Error("api: failed to list buckets", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	// Single query for all bucket stats instead of N+1.
	statsMap, err := s.repos.Objects.AggregateByBucket(ctx)
	if err != nil {
		s.logger.Warn("api: failed to aggregate object stats by bucket", "error", err)
		statsMap = make(map[int64]repository.BucketObjectStats)
	}

	items := make([]bucketListItem, 0, len(buckets))
	for _, b := range buckets {
		stats := statsMap[b.ID]
		items = append(items, bucketListItem{
			ID:             b.ID,
			Name:           b.Name,
			Status:         string(b.Status),
			ProofSetID:     b.ProofSetID,
			ObjectCount:    stats.Count,
			TotalSizeBytes: stats.TotalSize,
			CreatedAt:      b.CreatedAt.Format(time.RFC3339),
		})
	}

	writeJSON(w, http.StatusOK, items)
}

type objectListItem struct {
	ID          int64   `json:"id"`
	Key         string  `json:"key"`
	Size        int64   `json:"size"`
	State       string  `json:"state"`
	ContentType string  `json:"content_type"`
	ETag        string  `json:"etag"`
	PieceCID    *string `json:"piece_cid,omitempty"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
}

type objectListResponse struct {
	Objects    []objectListItem `json:"objects"`
	HasMore    bool             `json:"has_more"`
	NextMarker string           `json:"next_marker,omitempty"`
}

func (s *Server) handleAPIBucketObjects(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")

	bucket, err := s.repos.Buckets.GetByName(ctx, bucketName)
	if err != nil {
		s.logger.Error("api: failed to get bucket", "error", err, "name", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	if bucket == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}
	if !bucket.Status.IsVisible() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "bucket not found"})
		return
	}

	prefix := r.URL.Query().Get("prefix")
	after := r.URL.Query().Get("after")
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	objects, err := s.repos.Objects.ListByBucket(ctx, bucket.ID, prefix, after, limit+1)
	if err != nil {
		s.logger.Error("api: failed to list objects", "error", err, "bucket", bucketName)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	hasMore := len(objects) > limit
	if hasMore {
		objects = objects[:limit]
	}

	items := make([]objectListItem, 0, len(objects))
	for _, o := range objects {
		items = append(items, objectListItem{
			ID:          o.ID,
			Key:         o.Key,
			Size:        o.Size,
			State:       string(o.State),
			ContentType: o.ContentType,
			ETag:        o.ETag,
			PieceCID:    o.PieceCID,
			CreatedAt:   o.CreatedAt.Format(time.RFC3339),
			UpdatedAt:   o.UpdatedAt.Format(time.RFC3339),
		})
	}

	resp := objectListResponse{
		Objects: items,
		HasMore: hasMore,
	}
	if hasMore && len(items) > 0 {
		resp.NextMarker = items[len(items)-1].Key
	}

	writeJSON(w, http.StatusOK, resp)
}
