package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewS3Client(endpoint, accessKey, secretKey string) *awss3.Client {
	return awss3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
}

func NewUnixSocketS3Client(socketPath, accessKey, secretKey string) *awss3.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}
	awsConfig := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		HTTPClient:  &http.Client{Transport: transport},
	}
	return awss3.NewFromConfig(awsConfig, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String("http://synaps3.system.test")
		options.UsePathStyle = true
	})
}

func AssertS3Object(t testing.TB, ctx context.Context, client *awss3.Client, bucket, key string, want []byte, checksum [sha256.Size]byte) {
	t.Helper()
	output, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
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
