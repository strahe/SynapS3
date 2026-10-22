package synapse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/strahe/synapse-go/pdp"
	sdktypes "github.com/strahe/synapse-go/types"
)

// ProviderPiecesState separates explicit provider absence from failed observations.
type ProviderPiecesState struct {
	Known   []sdktypes.BigInt
	Unknown []sdktypes.BigInt
	Errors  map[string]error
}

func (s *StorageServiceAdapter) ProviderPiecesKnown(ctx context.Context, serviceURL string, dataSetID sdktypes.BigInt, pieceIDs []sdktypes.BigInt) (ProviderPiecesState, error) {
	base, err := parsePDPHTTPURL(serviceURL, "provider service URL")
	if err != nil || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return ProviderPiecesState{}, errors.New("invalid provider service URL")
	}
	base = cloneUploadBaseURL(base)
	client := s.providerHTTP
	if client == nil {
		client = NewProviderHTTPClient(storeControlTimeout, false)
	}
	result := ProviderPiecesState{Errors: make(map[string]error)}
	seen := make(map[string]struct{}, len(pieceIDs))
	for _, id := range pieceIDs {
		key := id.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		endpoint := base.ResolveReference(&url.URL{Path: "pdp/data-sets/" + dataSetID.String() + "/pieces/" + key})
		known, err := providerPieceKnown(ctx, client, endpoint, key)
		if err != nil {
			result.Errors[key] = err
		} else if known {
			result.Known = append(result.Known, id)
		} else {
			result.Unknown = append(result.Unknown, id)
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
	}
	return result, nil
}

func providerPieceKnown(ctx context.Context, client *http.Client, endpoint *url.URL, pieceID string) (bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, storeControlTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", pdp.DefaultUserAgent)
	resp, err := client.Do(req)
	if err != nil {
		return false, NormalizeProviderOperationError(ctx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, providerErrorResponseLimit))
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, readProviderHTTPError(req, resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, providerErrorResponseLimit+1))
	if err != nil {
		return false, err
	}
	if len(body) > providerErrorResponseLimit {
		return false, errors.New("provider piece response is too large")
	}
	var result struct {
		PieceID json.Number `json:"pieceId"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, fmt.Errorf("reading provider piece response: %w", err)
	}
	if result.PieceID.String() != pieceID {
		return false, fmt.Errorf("provider piece identity mismatch: got %q want %s", result.PieceID, pieceID)
	}
	return true, nil
}
