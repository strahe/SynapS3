package synapse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/strahe/synapse-go/pdp"
)

const defaultPDPStatusTimeout = 4 * time.Second

var errPDPStatusPrivateNetwork = errors.New("private network status URL blocked")

var pdpStatusSpecialUsePrefixes = []netip.Prefix{
	mustPDPStatusPrefix("100::/64"),
	mustPDPStatusPrefix("2001::/23"),
	mustPDPStatusPrefix("2001:db8::/32"),
	mustPDPStatusPrefix("2002::/16"),
	mustPDPStatusPrefix("3fff::/20"),
	mustPDPStatusPrefix("64:ff9b::/96"),
	mustPDPStatusPrefix("64:ff9b:1::/48"),
}

type PDPStatusState string

const (
	PDPStatusPending     PDPStatusState = "pending"
	PDPStatusConfirmed   PDPStatusState = "confirmed"
	PDPStatusRejected    PDPStatusState = "rejected"
	PDPStatusMismatch    PDPStatusState = "mismatch"
	PDPStatusUnavailable PDPStatusState = "unavailable"
	PDPStatusUnknown     PDPStatusState = "unknown"
)

type PDPStatusCheckerOptions struct {
	Timeout              time.Duration
	AllowPrivateNetworks bool
}

type PDPStatusChecker struct {
	timeout              time.Duration
	allowPrivateNetworks bool
	httpClient           *http.Client
}

func NewPDPStatusChecker(opts PDPStatusCheckerOptions) *PDPStatusChecker {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultPDPStatusTimeout
	}
	return &PDPStatusChecker{
		timeout:              timeout,
		allowPrivateNetworks: opts.AllowPrivateNetworks,
		httpClient:           newPDPStatusHTTPClient(timeout, opts.AllowPrivateNetworks),
	}
}

type DataSetCreationStatusInput struct {
	StatusURL         string
	TransactionID     string
	ExpectedDataSetID string
}

type AddPiecesStatusInput struct {
	ServiceURL         string
	StatusURL          string
	DataSetID          string
	TransactionID      string
	ExpectedPieceCount int
}

type PDPStatusResult struct {
	State             PDPStatusState
	StatusURL         string
	TxStatus          string
	DataSetID         string
	DataSetCreated    bool
	PiecesAdded       bool
	PieceCount        int
	ConfirmedPieceIDs []string
	Error             string
}

func (c *PDPStatusChecker) CheckDataSetCreationStatus(ctx context.Context, input DataSetCreationStatusInput) PDPStatusResult {
	statusURL := input.StatusURL
	result := PDPStatusResult{StatusURL: statusURL}
	if statusURL == "" {
		return result.withError(PDPStatusUnavailable, "missing status URL")
	}
	client, err := c.clientForStatusURL(statusURL)
	if err != nil {
		return result.withError(PDPStatusUnavailable, err.Error())
	}
	status, err := client.GetDataSetCreationStatus(ctx, statusURL)
	if err != nil {
		return result.withError(PDPStatusUnavailable, err.Error())
	}
	result.TxStatus = status.TxStatus
	result.DataSetCreated = status.DataSetCreated
	if status.DataSetID != nil {
		result.DataSetID = status.DataSetID.String()
	}
	if err := validateDataSetCreationStatusIdentity(input, result, status.CreateMessageHash.Hex()); err != nil {
		return result.withError(PDPStatusMismatch, err.Error())
	}
	result.State = classifyCreationStatus(status.TxStatus, status.DataSetCreated)
	return result
}

func (c *PDPStatusChecker) CheckAddPiecesStatus(ctx context.Context, input AddPiecesStatusInput) PDPStatusResult {
	statusURL := input.StatusURL
	if statusURL == "" {
		var err error
		statusURL, err = buildAddPiecesStatusURL(input.ServiceURL, input.DataSetID, input.TransactionID)
		if err != nil {
			return PDPStatusResult{}.withError(PDPStatusUnavailable, err.Error())
		}
	}
	result := PDPStatusResult{StatusURL: statusURL}
	if input.ExpectedPieceCount <= 0 {
		return result.withError(PDPStatusUnavailable, "missing expected piece count")
	}
	client, err := c.clientForStatusURL(statusURL)
	if err != nil {
		return result.withError(PDPStatusUnavailable, err.Error())
	}
	status, err := client.GetAddPiecesStatus(ctx, statusURL)
	if err != nil {
		return result.withError(PDPStatusUnavailable, err.Error())
	}
	result.TxStatus = status.TxStatus
	result.DataSetID = status.DataSetID.String()
	result.PieceCount = status.PieceCount
	result.PiecesAdded = status.PiecesAdded
	for _, id := range status.ConfirmedPieceIDs {
		result.ConfirmedPieceIDs = append(result.ConfirmedPieceIDs, id.String())
	}
	if err := validateAddPiecesStatusIdentity(input, result, status.TxHash.Hex()); err != nil {
		return result.withError(PDPStatusMismatch, err.Error())
	}
	result.State = classifyAddPiecesStatus(status.TxStatus, status.PiecesAdded, status.PieceCount, input.ExpectedPieceCount, len(result.ConfirmedPieceIDs))
	return result
}

func (c *PDPStatusChecker) clientForStatusURL(statusURL string) (*pdp.Client, error) {
	parsed, err := url.Parse(statusURL)
	if err != nil {
		return nil, fmt.Errorf("parse status URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("unsupported status URL scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("missing status URL host")
	}
	base := &url.URL{Scheme: parsed.Scheme, Host: parsed.Host}
	httpClient := c.httpClient
	if httpClient == nil {
		timeout := c.timeout
		if timeout <= 0 {
			timeout = defaultPDPStatusTimeout
		}
		httpClient = newPDPStatusHTTPClient(timeout, c.allowPrivateNetworks)
	}
	return pdp.New(base.String(),
		pdp.WithHTTPClient(httpClient),
		pdp.WithMaxRetries(0),
	)
}

func validateDataSetCreationStatusIdentity(input DataSetCreationStatusInput, result PDPStatusResult, statusTxHash string) error {
	if expected := normalizeStatusTxHash(input.TransactionID); expected != "" {
		got := normalizeStatusTxHash(statusTxHash)
		if !strings.EqualFold(got, expected) {
			return fmt.Errorf("status transaction ID mismatch: got %s want %s", statusTxHash, expected)
		}
	}
	confirmedCreated := result.TxStatus == "confirmed" && result.DataSetCreated
	if confirmedCreated && (strings.TrimSpace(result.DataSetID) == "" || strings.TrimSpace(result.DataSetID) == "0") {
		return fmt.Errorf("status data set ID missing for confirmed creation")
	}
	if expected := strings.TrimSpace(input.ExpectedDataSetID); expected != "" && strings.TrimSpace(result.DataSetID) != "" && strings.TrimSpace(result.DataSetID) != expected {
		return fmt.Errorf("status data set ID mismatch: got %s want %s", result.DataSetID, expected)
	}
	return nil
}

func validateAddPiecesStatusIdentity(input AddPiecesStatusInput, result PDPStatusResult, statusTxHash string) error {
	if expected := strings.TrimSpace(input.DataSetID); expected != "" && strings.TrimSpace(result.DataSetID) != expected {
		return fmt.Errorf("status data set ID mismatch: got %s want %s", result.DataSetID, expected)
	}
	if expected := normalizeStatusTxHash(input.TransactionID); expected != "" {
		got := normalizeStatusTxHash(statusTxHash)
		if !strings.EqualFold(got, expected) {
			return fmt.Errorf("status transaction ID mismatch: got %s want %s", statusTxHash, expected)
		}
	}
	return nil
}

func normalizeStatusTxHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if !strings.HasPrefix(value, "0x") && !strings.HasPrefix(value, "0X") {
		value = "0x" + value
	}
	return strings.ToLower(value)
}

func buildAddPiecesStatusURL(serviceURL, dataSetID, transactionID string) (string, error) {
	if serviceURL == "" {
		return "", fmt.Errorf("missing service URL")
	}
	if dataSetID == "" {
		return "", fmt.Errorf("missing data set ID")
	}
	if transactionID == "" {
		return "", fmt.Errorf("missing transaction ID")
	}
	base, err := url.Parse(serviceURL)
	if err != nil {
		return "", fmt.Errorf("parse service URL: %w", err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return "", fmt.Errorf("unsupported service URL scheme %q", base.Scheme)
	}
	base.Path = strings.TrimRight(base.Path, "/")
	path, err := url.JoinPath(base.Path, "pdp", "data-sets", dataSetID, "pieces", "added", transactionID)
	if err != nil {
		return "", fmt.Errorf("build add-pieces status path: %w", err)
	}
	base.Path = path
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
}

func classifyCreationStatus(txStatus string, dataSetCreated bool) PDPStatusState {
	switch txStatus {
	case "pending":
		return PDPStatusPending
	case "confirmed":
		if dataSetCreated {
			return PDPStatusConfirmed
		}
		return PDPStatusMismatch
	case "rejected":
		return PDPStatusRejected
	default:
		return PDPStatusUnknown
	}
}

func classifyAddPiecesStatus(txStatus string, piecesAdded bool, pieceCount, expectedPieceCount, confirmedPieceIDCount int) PDPStatusState {
	switch txStatus {
	case "pending":
		return PDPStatusPending
	case "confirmed":
		if !piecesAdded {
			return PDPStatusMismatch
		}
		if expectedPieceCount <= 0 {
			return PDPStatusMismatch
		}
		if pieceCount != expectedPieceCount {
			return PDPStatusMismatch
		}
		if confirmedPieceIDCount != expectedPieceCount {
			return PDPStatusMismatch
		}
		return PDPStatusConfirmed
	case "rejected":
		return PDPStatusRejected
	default:
		return PDPStatusUnknown
	}
}

func newPDPStatusHTTPClient(timeout time.Duration, allowPrivate bool) *http.Client {
	base := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           safePDPStatusDialContext(base, allowPrivate),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func safePDPStatusDialContext(base *net.Dialer, allowPrivate bool) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if ip := net.ParseIP(host); ip != nil {
			if !allowPrivate && isPrivatePDPStatusAddress(ip) {
				return nil, fmt.Errorf("%w: %s", errPDPStatusPrivateNetwork, ip)
			}
			return base.DialContext(ctx, network, addr)
		}

		resolver := base.Resolver
		if resolver == nil {
			resolver = net.DefaultResolver
		}
		ips, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		var firstErr error
		for _, resolved := range ips {
			if !allowPrivate && isPrivatePDPStatusAddress(resolved.IP) {
				if firstErr == nil {
					firstErr = fmt.Errorf("%w: %s resolves to %s", errPDPStatusPrivateNetwork, host, resolved.IP)
				}
				continue
			}
			conn, dialErr := base.DialContext(ctx, network, net.JoinHostPort(resolved.IP.String(), port))
			if dialErr == nil {
				return conn, nil
			}
			if firstErr == nil {
				firstErr = dialErr
			}
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("%w: no acceptable address for %s", errPDPStatusPrivateNetwork, host)
		}
		return nil, firstErr
	}
}

func isPrivatePDPStatusAddress(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsPrivate() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 0 {
			return true
		}
		if v4[0] == 100 && (v4[1]&0xc0) == 64 {
			return true
		}
		if v4[0] == 192 && v4[1] == 0 && v4[2] == 2 {
			return true
		}
		if v4[0] == 198 && v4[1] == 51 && v4[2] == 100 {
			return true
		}
		if v4[0] == 203 && v4[1] == 0 && v4[2] == 113 {
			return true
		}
		if v4[0] == 198 && (v4[1]&0xfe) == 18 {
			return true
		}
		if (v4[0] & 0xf0) == 0xf0 {
			return true
		}
	}
	if addr, ok := netip.AddrFromSlice(ip); ok {
		addr = addr.Unmap()
		for _, prefix := range pdpStatusSpecialUsePrefixes {
			if prefix.Contains(addr) {
				return true
			}
		}
	}
	return false
}

func mustPDPStatusPrefix(value string) netip.Prefix {
	prefix, err := netip.ParsePrefix(value)
	if err != nil {
		panic(err)
	}
	return prefix
}

func (r PDPStatusResult) withError(state PDPStatusState, message string) PDPStatusResult {
	r.State = state
	r.Error = message
	return r
}
