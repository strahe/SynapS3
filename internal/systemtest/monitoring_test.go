//go:build systemtest

package systemtest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/tests/testutil/e2e"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	"github.com/strahe/synapse-go/warmstorage"
)

func TestRuntimeMonitoringUsesStorageAdapterCounters(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte{0xab}, 4096)
	info, err := piece.CalculateFromBytes(payload)
	if err != nil {
		t.Fatal(err)
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	t.Cleanup(provider.Close)
	service, err := storage.New(storage.Options{HTTPClient: provider.Client()})
	if err != nil {
		t.Fatal(err)
	}
	chainClient := ethclient.NewClient(rpc.DialInProc(rpc.NewServer()))
	t.Cleanup(chainClient.Close)
	identity := (&MemoryFilecoin{}).ContextIdentity()
	dataSets, err := warmstorage.New(warmstorage.Options{
		Client: chainClient, FWSS: identity.RecordKeeper,
		ViewContract: common.HexToAddress("0x00000000000000000000000000000000000000e5"),
		PDPVerifier:  (&MemoryFilecoin{}).PDPVerifierAddress(),
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter := synapse.AdaptStorageService(service, dataSets, identity)
	harness, err := NewHarness(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), HarnessOptions{Storage: adapter})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := harness.Close(ctx); err != nil {
			t.Errorf("Close harness: %v", err)
		}
	})

	admin := e2e.NewAdminClient(t, harness.AdminURL)
	admin.Login(t, t.Context(), AdminUsername, AdminPassword)
	var snapshot observability.MonitoringSnapshot
	if _, err := admin.GetJSON(t.Context(), "/api/v1/monitoring", &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.SampledAt == nil || len(snapshot.Points) == 0 {
		t.Fatal("runtime has no monitoring baseline")
	}
	remote, err := adapter.Download(t.Context(), info.CIDv2, &storage.DownloadOptions{URL: provider.URL})
	if err != nil {
		t.Fatal(err)
	}
	read, readErr := io.Copy(io.Discard, remote)
	closeErr := remote.Close()
	if read != int64(len(payload)) || readErr != nil || closeErr != nil {
		t.Fatalf("provider payload: bytes=%d, read=%v, close=%v", read, readErr, closeErr)
	}
	metrics, err := admin.GetJSON(t.Context(), "/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics, `synaps3_transfer_bytes_total{direction="download",scope="storage"} 4096`+"\n") {
		t.Fatal("runtime metrics do not include consumed adapter bytes")
	}
}
