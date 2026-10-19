package app

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
)

func TestS3ServerTLSHealthAndShutdown(t *testing.T) {
	t.Parallel()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, publicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("Write certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
		t.Fatalf("Write private key: %v", err)
	}
	opts, err := s3ServerOptions(config.ServerConfig{
		MaxConnections: 10,
		MaxRequests:    10,
		TLS:            config.TLSConfig{Enabled: true, CertFile: certFile, KeyFile: keyFile},
	})
	if err != nil {
		t.Fatalf("s3ServerOptions: %v", err)
	}
	bound := make(chan []net.Addr, 1)
	opts = append(opts, s3api.WithOnListenAddrs(func(addrs []net.Addr) { bound <- addrs }))
	root := auth.Account{Access: "test-access", Secret: "test-secret"}
	server, err := s3api.New(backend.BackendUnsupported{}, middlewares.RootUserConfig{Access: root.Access, Secret: root.Secret},
		"us-east-1", auth.NewIAMServiceSingle(root), nil, nil, nil, nil, opts...)
	if err != nil {
		t.Fatalf("s3api.New: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- server.ServeMultiPort([]string{"127.0.0.1:0"}) }()
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			if err := server.ShutDown(); err != nil {
				t.Errorf("ShutDown: %v", err)
			}
		}
	})
	var address string
	select {
	case addrs := <-bound:
		if len(addrs) != 1 {
			t.Fatalf("bound addresses = %v, want one", addrs)
		}
		address = addrs[0].String()
	case err := <-served:
		t.Fatalf("ServeMultiPort returned before binding: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("S3 server did not report its bound address")
	}
	trusted, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(trusted)
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+address+"/health", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET HTTPS health: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("HTTPS health status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if err := server.ShutDown(); err != nil {
		t.Fatalf("ShutDown: %v", err)
	}
	stopped = true
	select {
	case err := <-served:
		if err != nil && err.Error() != "listener closed" {
			t.Fatalf("ServeMultiPort after shutdown: %v", err)
		}
	case <-time.After(defaultShutdownTimeout + time.Second):
		t.Fatal("S3 server did not exit after shutdown")
	}
}
