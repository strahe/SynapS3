//go:build systemtest

package system_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/systemtest"
)

const csrfHeader = "X-SynapS3-CSRF"

type adminClient struct {
	baseURL string
	client  *http.Client
	csrf    string
}

type objectListResponse struct {
	Objects []struct {
		Key              string `json:"key"`
		CurrentVersionID string `json:"current_version_id"`
		State            string `json:"state"`
		Status           string `json:"status"`
		UploadStatus     string `json:"upload_status"`
		Location         struct {
			Cache    bool `json:"cache"`
			Filecoin bool `json:"filecoin"`
		} `json:"location"`
	} `json:"objects"`
}

type provenanceResponse struct {
	Status          string `json:"status"`
	UploadStatus    string `json:"upload_status"`
	RequestedCopies int    `json:"requested_copies"`
	SuccessCopies   int    `json:"success_copies"`
	Copies          []struct {
		Status       string `json:"status"`
		ProviderID   string `json:"provider_id"`
		DataSetID    string `json:"data_set_id"`
		PieceID      string `json:"piece_id"`
		RetrievalURL string `json:"retrieval_url"`
	} `json:"copies"`
}

func TestSystemGoldenPath(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	harness, err := systemtest.NewHarness(t.Context(), logger)
	if err != nil {
		t.Fatalf("NewHarness: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if closed {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := harness.Close(closeCtx); err != nil {
			t.Errorf("Close harness: %v", err)
		}
	})

	admin := newAdminClient(t, harness.AdminURL)
	admin.login(t, systemtest.AdminUsername, systemtest.AdminPassword)
	var credentials struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
	}
	admin.postJSON(t, "/api/v1/s3-users", map[string]string{"role": "userplus"}, &credentials)
	if credentials.AccessKey == "" || credentials.SecretKey == "" {
		t.Fatalf("created S3 credentials are incomplete: %#v", credentials)
	}

	s3Client := harness.S3Client(credentials.AccessKey, credentials.SecretKey)
	bucket, key := "system-golden", "objects/golden.bin"
	if _, err := s3Client.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	content := bytes.Repeat([]byte("synaps3-system-golden\n"), 6000)
	checksum := sha256.Sum256(content)
	if _, err := s3Client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(content), ContentType: aws.String("application/octet-stream"),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	assertS3Object(t, s3Client, bucket, key, content, checksum)

	object := eventually(t, 10*time.Second, "object to complete three-copy upload", func() (struct {
		VersionID string
		Snapshot  string
	}, bool, error,
	) {
		var list objectListResponse
		raw, err := admin.getJSON("/api/v1/buckets/"+bucket+"/objects?prefix="+url.QueryEscape(key), &list)
		if err != nil {
			return struct {
				VersionID string
				Snapshot  string
			}{Snapshot: raw}, false, err
		}
		if len(list.Objects) != 1 {
			return struct {
				VersionID string
				Snapshot  string
			}{Snapshot: raw}, false, nil
		}
		item := list.Objects[0]
		return struct {
			VersionID string
			Snapshot  string
		}{VersionID: item.CurrentVersionID, Snapshot: raw}, item.UploadStatus == "complete" && item.Location.Filecoin, nil
	})

	provenancePath := "/api/v1/buckets/" + bucket + "/objects/provenance?version_id=" + url.QueryEscape(object.VersionID)
	provenance := eventually(t, 5*time.Second, "three readable committed copies", func() (provenanceResponse, bool, error) {
		var value provenanceResponse
		_, err := admin.getJSON(provenancePath, &value)
		if err != nil {
			return value, false, err
		}
		if value.SuccessCopies != 3 || value.RequestedCopies != 3 || len(value.Copies) != 3 {
			return value, false, nil
		}
		for _, copy := range value.Copies {
			if copy.Status != "committed" || copy.ProviderID == "" || copy.DataSetID == "" || copy.PieceID == "" || copy.RetrievalURL == "" {
				return value, false, nil
			}
		}
		return value, true, nil
	})
	if provenance.UploadStatus != "complete" {
		t.Fatalf("provenance upload status = %q, want complete", provenance.UploadStatus)
	}

	eventually(t, 5*time.Second, "completed upload tasks", func() (string, bool, error) {
		var tasks struct {
			Tasks []struct {
				Status       string `json:"status"`
				RefVersionID string `json:"ref_version_id"`
			} `json:"tasks"`
		}
		raw, err := admin.getJSON("/api/v1/tasks?type=upload&limit=100", &tasks)
		if err != nil {
			return raw, false, err
		}
		completed, failed := 0, 0
		for _, task := range tasks.Tasks {
			if task.RefVersionID != object.VersionID {
				continue
			}
			switch task.Status {
			case "completed":
				completed++
			case "failed":
				failed++
			}
		}
		return raw, completed > 0 && failed == 0, nil
	})

	eventually(t, 5*time.Second, "automatic cache eviction", func() (string, bool, error) {
		var list objectListResponse
		raw, err := admin.getJSON("/api/v1/buckets/"+bucket+"/objects?prefix="+url.QueryEscape(key), &list)
		if err != nil {
			return raw, false, err
		}
		ready := len(list.Objects) == 1 && !list.Objects[0].Location.Cache && list.Objects[0].Location.Filecoin
		return raw, ready, nil
	})
	assertS3Object(t, s3Client, bucket, key, content, checksum)

	eventually(t, 5*time.Second, "provider and dataset observability snapshots", func() (string, bool, error) {
		var providers struct {
			Summary struct {
				Available int `json:"available"`
			} `json:"summary"`
		}
		providerRaw, err := admin.getJSON("/api/v1/observability/providers?limit=20", &providers)
		if err != nil {
			return providerRaw, false, err
		}
		var dataSets struct {
			Summary struct {
				Available int `json:"available"`
			} `json:"summary"`
		}
		dataSetRaw, err := admin.getJSON("/api/v1/observability/data-sets?limit=20", &dataSets)
		if err != nil {
			return providerRaw + "\n" + dataSetRaw, false, err
		}
		return providerRaw + "\n" + dataSetRaw, providers.Summary.Available == 3 && dataSets.Summary.Available == 3, nil
	})

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := harness.Close(closeCtx); err != nil {
		t.Fatalf("Close harness: %v", err)
	}
	closed = true
}

func newAdminClient(t *testing.T, baseURL string) *adminClient {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	return &adminClient{baseURL: baseURL, client: &http.Client{Jar: jar, Timeout: 5 * time.Second}}
}

func (a *adminClient) login(t *testing.T, username, password string) {
	t.Helper()
	var session struct {
		CSRFToken string `json:"csrf_token"`
	}
	a.postJSON(t, "/api/v1/auth/login", map[string]string{"username": username, "password": password}, &session)
	if session.CSRFToken == "" {
		t.Fatal("admin login returned an empty CSRF token")
	}
	a.csrf = session.CSRFToken
}

func (a *adminClient) postJSON(t *testing.T, path string, payload, output any) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", path, err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, a.baseURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create %s request: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if a.csrf != "" {
		req.Header.Set(csrfHeader, a.csrf)
	}
	response, err := a.client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read POST %s: %v", path, err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("POST %s status=%d body=%s", path, response.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, output); err != nil {
		t.Fatalf("decode POST %s: %v; body=%s", path, err, raw)
	}
}

func (a *adminClient) getJSON(path string, output any) (string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, a.baseURL+path, nil)
	if err != nil {
		return "", err
	}
	response, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return string(raw), err
	}
	if response.StatusCode != http.StatusOK {
		return string(raw), fmt.Errorf("GET %s status=%d", path, response.StatusCode)
	}
	if err := json.Unmarshal(raw, output); err != nil {
		return string(raw), err
	}
	return string(raw), nil
}

func assertS3Object(t *testing.T, client *awss3.Client, bucket, key string, want []byte, checksum [sha256.Size]byte) {
	t.Helper()
	output, err := client.GetObject(t.Context(), &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() { _ = output.Body.Close() }()
	got, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatalf("read GetObject: %v", err)
	}
	if !bytes.Equal(got, want) || sha256.Sum256(got) != checksum {
		t.Fatal("GetObject content or checksum differs from uploaded object")
	}
}

func eventually[T any](t *testing.T, timeout time.Duration, description string, poll func() (T, bool, error)) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var last T
	var lastErr error
	for {
		value, ready, err := poll()
		last, lastErr = value, err
		if err == nil && ready {
			return value
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: last=%s error=%v", description, diagnosticValue(last), lastErr)
		case <-ticker.C:
		}
	}
}

func diagnosticValue(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	return strings.TrimSpace(string(raw))
}
