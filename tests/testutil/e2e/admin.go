package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"
	"time"
)

const csrfHeader = "X-SynapS3-CSRF"

// AdminClient is a small black-box client for the SynapS3 Admin API.
type AdminClient struct {
	baseURL string
	client  *http.Client
	csrf    string
}

type AdminClientOption func(*AdminClient)

func WithAdminTimeout(timeout time.Duration) AdminClientOption {
	return func(client *AdminClient) {
		if timeout > 0 {
			client.client.Timeout = timeout
		}
	}
}

type HealthResponse struct {
	Status string   `json:"status"`
	Errors []string `json:"errors,omitempty"`
}

type SettingsResponse struct {
	Mode             string `json:"mode"`
	RuntimeAvailable bool   `json:"runtime_available"`
}

type S3Credentials struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Role      string `json:"role"`
}

type ObjectListResponse struct {
	Objects []ObjectListItem `json:"objects"`
}

type ObjectListItem struct {
	Key              string         `json:"key"`
	CurrentVersionID string         `json:"current_version_id"`
	State            string         `json:"state"`
	Status           string         `json:"status"`
	UploadStatus     string         `json:"upload_status"`
	Location         ObjectLocation `json:"location"`
}

type ObjectLocation struct {
	Cache    bool `json:"cache"`
	Filecoin bool `json:"filecoin"`
}

type ProvenanceResponse struct {
	Status          string           `json:"status"`
	UploadStatus    string           `json:"upload_status"`
	RequestedCopies int              `json:"requested_copies"`
	SuccessCopies   int              `json:"success_copies"`
	Copies          []ProvenanceCopy `json:"copies"`
}

type ProvenanceCopy struct {
	Status       string `json:"status"`
	ProviderID   string `json:"provider_id"`
	DataSetID    string `json:"data_set_id"`
	PieceID      string `json:"piece_id"`
	RetrievalURL string `json:"retrieval_url"`
}

type TaskListResponse struct {
	Tasks []TaskItem `json:"tasks"`
}

type TaskItem struct {
	ID            int64   `json:"id"`
	Type          string  `json:"type"`
	Stage         *string `json:"stage,omitempty"`
	Status        string  `json:"status"`
	RefVersionID  string  `json:"ref_version_id"`
	RetryCount    int     `json:"retry_count"`
	LastError     *string `json:"last_error,omitempty"`
	StatusMessage *string `json:"status_message,omitempty"`
	WaitReason    *string `json:"wait_reason,omitempty"`
	ClaimedAt     *string `json:"claimed_at,omitempty"`
}

type ReadinessResult struct {
	Status        string            `json:"status"`
	Mode          string            `json:"mode"`
	Checks        []ReadinessCheck  `json:"checks"`
	PartialErrors map[string]string `json:"partial_errors,omitempty"`
}

type ReadinessCheck struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	Action        string `json:"action,omitempty"`
	RequiredUSDFC string `json:"required_usdfc,omitempty"`
}

type WalletResponse struct {
	WalletBalances *WalletBalances `json:"wallet_balances,omitempty"`
}

type WalletBalances struct {
	USDFC *string `json:"usdfc"`
}

type WalletOperationResponse struct {
	Operation WalletOperation `json:"operation"`
}

type WalletOperationsResponse struct {
	Operations []WalletOperation `json:"operations"`
}

type WalletOperation struct {
	ID              int64   `json:"id"`
	Type            string  `json:"type"`
	ClientRequestID string  `json:"client_request_id"`
	Amount          string  `json:"amount"`
	Status          string  `json:"status"`
	TxHash          *string `json:"tx_hash,omitempty"`
	LastError       *string `json:"last_error,omitempty"`
}

type ObservabilitySummary struct {
	Total       int `json:"total"`
	Available   int `json:"available"`
	Degraded    int `json:"degraded"`
	Unavailable int `json:"unavailable"`
	Unknown     int `json:"unknown"`
}

type ProviderObservationPage struct {
	Items   []ProviderObservation `json:"items"`
	Summary ObservabilitySummary  `json:"summary"`
}

type ProviderObservation struct {
	Facts struct {
		ProviderID   string  `json:"provider_id"`
		Active       *bool   `json:"active,omitempty"`
		HasPDP       *bool   `json:"has_pdp,omitempty"`
		ServiceURL   *string `json:"service_url,omitempty"`
		HealthStatus *string `json:"health_status,omitempty"`
	} `json:"facts"`
	Signal struct {
		Status      string   `json:"status"`
		Level       string   `json:"level"`
		ReasonCodes []string `json:"reason_codes"`
		LastError   *string  `json:"last_error,omitempty"`
	} `json:"signal"`
}

type DataSetObservationPage struct {
	Items   []DataSetObservation `json:"items"`
	Summary ObservabilitySummary `json:"summary"`
}

type DataSetObservation struct {
	Facts struct {
		BucketName     string  `json:"bucket_name"`
		ProviderID     string  `json:"provider_id"`
		ChainDataSetID *string `json:"chain_data_set_id,omitempty"`
	} `json:"facts"`
	Signal struct {
		Status      string   `json:"status"`
		Level       string   `json:"level"`
		ReasonCodes []string `json:"reason_codes"`
		LastError   *string  `json:"last_error,omitempty"`
	} `json:"signal"`
}

// NewAdminClient creates an Admin API client with cookie support.
func NewAdminClient(t testing.TB, baseURL string, options ...AdminClientOption) *AdminClient {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	client := &AdminClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Jar: jar, Timeout: 15 * time.Second},
	}
	for _, option := range options {
		option(client)
	}
	return client
}

func (a *AdminClient) BaseURL() string { return a.baseURL }

func (a *AdminClient) Login(t testing.TB, ctx context.Context, username, password string) {
	t.Helper()
	if err := a.Authenticate(ctx, username, password); err != nil {
		t.Fatalf("admin login: %v", err)
	}
}

func (a *AdminClient) Authenticate(ctx context.Context, username, password string) error {
	var session struct {
		CSRFToken string `json:"csrf_token"`
	}
	if _, err := a.DoJSON(ctx, http.MethodPost, "/api/v1/auth/login", map[string]string{"username": username, "password": password}, &session); err != nil {
		return err
	}
	if session.CSRFToken == "" {
		return fmt.Errorf("empty CSRF token")
	}
	a.csrf = session.CSRFToken
	return nil
}

func (a *AdminClient) CreateS3User(t testing.TB, ctx context.Context) S3Credentials {
	t.Helper()
	var credentials S3Credentials
	a.PostJSON(t, ctx, "/api/v1/s3-users", map[string]string{"role": "userplus"}, &credentials)
	if credentials.AccessKey == "" || credentials.SecretKey == "" {
		t.Fatalf("created S3 credentials are incomplete: %s", DiagnosticValue(credentials))
	}
	return credentials
}

func (a *AdminClient) Health(ctx context.Context) (HealthResponse, string, error) {
	var health HealthResponse
	raw, err := a.GetJSON(ctx, "/healthz", &health)
	return health, raw, err
}

func (a *AdminClient) Settings(ctx context.Context) (SettingsResponse, string, error) {
	var settings SettingsResponse
	raw, err := a.GetJSON(ctx, "/api/v1/settings", &settings)
	return settings, raw, err
}

func (a *AdminClient) PostJSON(t testing.TB, ctx context.Context, path string, payload, output any) {
	t.Helper()
	if _, err := a.DoJSON(ctx, http.MethodPost, path, payload, output); err != nil {
		t.Fatalf("%s %s: %v", http.MethodPost, path, err)
	}
}

func (a *AdminClient) GetJSON(ctx context.Context, path string, output any) (string, error) {
	return a.DoJSON(ctx, http.MethodGet, path, nil, output)
}

func (a *AdminClient) DoJSON(ctx context.Context, method, path string, payload, output any) (string, error) {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return "", fmt.Errorf("marshal payload: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if a.csrf != "" {
		req.Header.Set(csrfHeader, a.csrf)
	}
	response, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return string(raw), fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return string(raw), fmt.Errorf("status=%d body=%s", response.StatusCode, Redact(string(raw)))
	}
	if output == nil || len(bytes.TrimSpace(raw)) == 0 {
		return string(raw), nil
	}
	if err := json.Unmarshal(raw, output); err != nil {
		return string(raw), fmt.Errorf("decode response: %w; body=%s", err, Redact(string(raw)))
	}
	return string(raw), nil
}

func (r ReadinessResult) Check(id string) (ReadinessCheck, bool) {
	for _, check := range r.Checks {
		if check.ID == id {
			return check, true
		}
	}
	return ReadinessCheck{}, false
}
