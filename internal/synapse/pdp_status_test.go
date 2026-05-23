package synapse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	testCreateDataSetTxHash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	testAddPiecesTxHash     = "0x7890abcdef1234567890abcdef1234567890abcdef1234567890abcdef123456"
)

func TestPDPStatusCheckerChecksDataSetCreationStatusOnce(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/pdp/data-sets/created/0xabc" {
			t.Fatalf("path = %q, want creation status path", r.URL.Path)
		}
		_, _ = fmt.Fprintf(w, `{"createMessageHash":%q,"service":"svc","txStatus":"pending","dataSetCreated":false,"ok":null}`, testCreateDataSetTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckDataSetCreationStatus(t.Context(), DataSetCreationStatusInput{
		StatusURL:     server.URL + "/pdp/data-sets/created/0xabc",
		TransactionID: testCreateDataSetTxHash,
	})

	if got.State != PDPStatusPending || got.TxStatus != "pending" || got.StatusURL == "" || got.Error != "" {
		t.Fatalf("status = %#v, want pending without error", got)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want single status request", requests)
	}
}

func TestPDPStatusCheckerChecksAddPiecesStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/pdp/data-sets/1001/pieces/added/"+testAddPiecesTxHash {
			t.Fatalf("path = %q, want add-pieces status path", r.URL.Path)
		}
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"addMessageOk":true,"piecesAdded":true,"confirmedPieceIds":[2001]}`, testAddPiecesTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		ServiceURL:         server.URL,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if got.State != PDPStatusConfirmed || got.TxStatus != "confirmed" || !got.PiecesAdded || len(got.ConfirmedPieceIDs) != 1 || got.ConfirmedPieceIDs[0] != "2001" {
		t.Fatalf("status = %#v, want confirmed add-pieces result", got)
	}
}

func TestPDPStatusCheckerClassifiesAddPiecesMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"addMessageOk":null,"piecesAdded":false}`, testAddPiecesTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		ServiceURL:         server.URL,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if got.State != PDPStatusMismatch {
		t.Fatalf("state = %s, want mismatch: %#v", got.State, got)
	}
}

func TestPDPStatusCheckerClassifiesAddPiecesIdentityMismatch(t *testing.T) {
	tests := []struct {
		name          string
		response      string
		dataSetID     string
		transactionID string
	}{
		{
			name:          "data set mismatch",
			response:      fmt.Sprintf(`{"txHash":%q,"txStatus":"confirmed","dataSetId":9999,"pieceCount":1,"addMessageOk":true,"piecesAdded":true}`, testAddPiecesTxHash),
			dataSetID:     "1001",
			transactionID: testAddPiecesTxHash,
		},
		{
			name:          "transaction mismatch",
			response:      `{"txHash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"addMessageOk":true,"piecesAdded":true}`,
			dataSetID:     "1001",
			transactionID: testAddPiecesTxHash,
		},
		{
			name:          "missing data set id",
			response:      fmt.Sprintf(`{"txHash":%q,"txStatus":"confirmed","pieceCount":1,"addMessageOk":true,"piecesAdded":true}`, testAddPiecesTxHash),
			dataSetID:     "1001",
			transactionID: testAddPiecesTxHash,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, tt.response)
			}))
			defer server.Close()

			checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
			got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
				ServiceURL:         server.URL,
				DataSetID:          tt.dataSetID,
				TransactionID:      tt.transactionID,
				ExpectedPieceCount: 1,
			})

			if got.State != PDPStatusMismatch || got.Error == "" {
				t.Fatalf("status = %#v, want mismatch with identity error", got)
			}
		})
	}
}

func TestPDPStatusCheckerClassifiesRejectedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"createMessageHash":%q,"service":"svc","txStatus":"rejected","dataSetCreated":false,"ok":false}`, testCreateDataSetTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckDataSetCreationStatus(t.Context(), DataSetCreationStatusInput{
		StatusURL:     server.URL + "/pdp/data-sets/created/0xabc",
		TransactionID: testCreateDataSetTxHash,
	})

	if got.State != PDPStatusRejected || got.TxStatus != "rejected" {
		t.Fatalf("status = %#v, want rejected", got)
	}
}

func TestPDPStatusCheckerClassifiesTimeoutUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"pending","dataSetId":1001,"pieceCount":1,"piecesAdded":false}`, testAddPiecesTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: 5 * time.Millisecond, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		ServiceURL:         server.URL,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if got.State != PDPStatusUnavailable || got.Error == "" {
		t.Fatalf("status = %#v, want unavailable with error", got)
	}
}

func TestPDPStatusCheckerClassifiesMalformedResponseUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{not-json`)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		ServiceURL:         server.URL,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if got.State != PDPStatusUnavailable || got.Error == "" {
		t.Fatalf("status = %#v, want unavailable malformed response", got)
	}
}

func TestPDPStatusCheckerClassifiesDataSetCreationIdentityMismatch(t *testing.T) {
	tests := []struct {
		name              string
		response          string
		transactionID     string
		expectedDataSetID string
	}{
		{
			name:          "transaction mismatch",
			response:      `{"createMessageHash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","service":"svc","txStatus":"confirmed","dataSetCreated":true,"ok":true,"dataSetId":42}`,
			transactionID: testCreateDataSetTxHash,
		},
		{
			name:          "missing data set id",
			response:      fmt.Sprintf(`{"createMessageHash":%q,"service":"svc","txStatus":"confirmed","dataSetCreated":true,"ok":true}`, testCreateDataSetTxHash),
			transactionID: testCreateDataSetTxHash,
		},
		{
			name:          "zero data set id",
			response:      fmt.Sprintf(`{"createMessageHash":%q,"service":"svc","txStatus":"confirmed","dataSetCreated":true,"ok":true,"dataSetId":0}`, testCreateDataSetTxHash),
			transactionID: testCreateDataSetTxHash,
		},
		{
			name:              "expected data set mismatch",
			response:          fmt.Sprintf(`{"createMessageHash":%q,"service":"svc","txStatus":"confirmed","dataSetCreated":true,"ok":true,"dataSetId":42}`, testCreateDataSetTxHash),
			transactionID:     testCreateDataSetTxHash,
			expectedDataSetID: "99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, tt.response)
			}))
			defer server.Close()

			checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
			got := checker.CheckDataSetCreationStatus(t.Context(), DataSetCreationStatusInput{
				StatusURL:         server.URL + "/pdp/data-sets/created/" + tt.transactionID,
				TransactionID:     tt.transactionID,
				ExpectedDataSetID: tt.expectedDataSetID,
			})

			if got.State != PDPStatusMismatch || got.Error == "" {
				t.Fatalf("status = %#v, want mismatch with identity error", got)
			}
		})
	}
}

func TestPDPStatusCheckerClassifiesAddPiecesConfirmedPieceIDsMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"addMessageOk":true,"piecesAdded":true,"confirmedPieceIds":[]}`, testAddPiecesTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		ServiceURL:         server.URL,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if got.State != PDPStatusMismatch {
		t.Fatalf("status = %#v, want mismatch for confirmedPieceIDs count", got)
	}
}

func TestPDPStatusCheckerRejectsInvalidExpectedPieceCountBeforeRequest(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":0,"addMessageOk":true,"piecesAdded":true,"confirmedPieceIds":[]}`, testAddPiecesTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		ServiceURL:         server.URL,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 0,
	})

	if requests != 0 {
		t.Fatalf("requests = %d, want none for invalid expected piece count", requests)
	}
	if got.State != PDPStatusUnavailable || !strings.Contains(got.Error, "expected piece count") {
		t.Fatalf("status = %#v, want unavailable expected piece count error", got)
	}
}

func TestPDPStatusCheckerBlocksPrivateStatusURLByDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"pending","dataSetId":1001,"pieceCount":1,"piecesAdded":false}`, testAddPiecesTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		StatusURL:          server.URL + "/pdp/data-sets/1001/pieces/added/" + testAddPiecesTxHash,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if got.State != PDPStatusUnavailable || !strings.Contains(got.Error, "private network") {
		t.Fatalf("status = %#v, want private network unavailable", got)
	}
}

func TestPDPStatusCheckerAllowsPrivateStatusURLWhenConfigured(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"pending","dataSetId":1001,"pieceCount":1,"piecesAdded":false}`, testAddPiecesTxHash)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		StatusURL:          server.URL + "/pdp/data-sets/1001/pieces/added/" + testAddPiecesTxHash,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if got.State != PDPStatusPending || got.Error != "" {
		t.Fatalf("status = %#v, want pending status through allowed private URL", got)
	}
}

func TestPDPStatusCheckerDoesNotUseEnvironmentProxy(t *testing.T) {
	var proxyRequests int
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyRequests++
		_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"addMessageOk":true,"piecesAdded":true,"confirmedPieceIds":[2001]}`, testAddPiecesTxHash)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("http_proxy", proxy.URL)

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: 50 * time.Millisecond, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		StatusURL:          "http://proxy-only.invalid/pdp/data-sets/1001/pieces/added/" + testAddPiecesTxHash,
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if proxyRequests != 0 {
		t.Fatalf("proxy requests = %d, want none", proxyRequests)
	}
	if got.State != PDPStatusUnavailable {
		t.Fatalf("status = %#v, want unavailable without environment proxy", got)
	}
}

func TestPDPStatusCheckerDoesNotFollowRedirects(t *testing.T) {
	var redirected int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirected" {
			redirected++
			_, _ = fmt.Fprintf(w, `{"txHash":%q,"txStatus":"confirmed","dataSetId":1001,"pieceCount":1,"addMessageOk":true,"piecesAdded":true,"confirmedPieceIds":[2001]}`, testAddPiecesTxHash)
			return
		}
		http.Redirect(w, r, "/redirected", http.StatusFound)
	}))
	defer server.Close()

	checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second, AllowPrivateNetworks: true})
	got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
		StatusURL:          server.URL + "/start",
		DataSetID:          "1001",
		TransactionID:      testAddPiecesTxHash,
		ExpectedPieceCount: 1,
	})

	if redirected != 0 {
		t.Fatalf("redirected requests = %d, want none", redirected)
	}
	if got.State != PDPStatusUnavailable {
		t.Fatalf("status = %#v, want unavailable without following redirects", got)
	}
}

func TestPDPStatusCheckerBlocksIPv6SpecialUseAddresses(t *testing.T) {
	tests := []string{
		"100::1",
		"2001::1",
		"2001:2::1",
		"2001:db8::1",
		"2002::1",
		"64:ff9b::1",
	}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			checker := NewPDPStatusChecker(PDPStatusCheckerOptions{Timeout: time.Second})
			got := checker.CheckAddPiecesStatus(t.Context(), AddPiecesStatusInput{
				StatusURL:          "http://[" + value + "]:80/pdp/data-sets/1001/pieces/added/" + testAddPiecesTxHash,
				DataSetID:          "1001",
				TransactionID:      testAddPiecesTxHash,
				ExpectedPieceCount: 1,
			})

			if got.State != PDPStatusUnavailable || !strings.Contains(got.Error, "private network") {
				t.Fatalf("status = %#v, want private network unavailable", got)
			}
		})
	}
}
