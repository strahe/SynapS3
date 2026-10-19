//go:build systemtest && s3compat

package system_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/big"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/systemtest"
	"github.com/strahe/synaps3/tests/testutil/e2e"
)

func testMatrixACLAuthorization(t *testing.T, harness *systemtest.Harness, owner *s3.Client) {
	admin := e2e.NewAdminClient(t, harness.AdminURL)
	admin.Login(t, t.Context(), systemtest.AdminUsername, systemtest.AdminPassword)
	credentials := admin.CreateS3User(t, t.Context())
	grantee := e2e.NewUnixSocketS3Client(harness.S3SocketPath(), credentials.AccessKey, credentials.SecretKey)
	var adminCredentials e2e.S3Credentials
	admin.PostJSON(t, t.Context(), "/api/v1/s3-users", map[string]string{"role": "admin"}, &adminCredentials)
	s3Admin := e2e.NewUnixSocketS3Client(harness.S3SocketPath(), adminCredentials.AccessKey, adminCredentials.SecretKey)
	bucket := "s3-matrix-acl"
	if _, err := owner.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatal(err)
	}
	waitForBucketReady(t, admin, bucket)
	acl, err := owner.GetBucketAcl(t.Context(), &s3.GetBucketAclInput{Bucket: &bucket})
	if err != nil || acl.Owner == nil {
		t.Fatalf("bucket ACL = %#v, %v", acl, err)
	}
	payload := bytes.Repeat([]byte("acl content\n"), 32)
	for _, permission := range []types.Permission{types.PermissionRead, types.PermissionWrite, types.PermissionFullControl} {
		t.Run(string(permission), func(t *testing.T) {
			ctx := t.Context()
			_, err := owner.PutBucketAcl(ctx, &s3.PutBucketAclInput{
				Bucket: &bucket,
				AccessControlPolicy: &types.AccessControlPolicy{Owner: acl.Owner, Grants: []types.Grant{
					{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: acl.Owner.ID}, Permission: types.PermissionFullControl},
					{Grantee: &types.Grantee{Type: types.TypeCanonicalUser, ID: &credentials.AccessKey}, Permission: permission},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			key := strings.ToLower(string(permission)) + ".bin"
			version := matrixPutObject(t, owner, bucket, key, payload).VersionId
			canRead := permission != types.PermissionWrite
			canWrite := permission != types.PermissionRead
			out, err := grantee.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
			if canRead {
				if err != nil {
					t.Fatal(err)
				}
				got, readErr := io.ReadAll(out.Body)
				_ = out.Body.Close()
				if readErr != nil || !bytes.Equal(got, payload) {
					t.Fatalf("shared GET = %q, %v", got, readErr)
				}
			} else {
				requireS3ErrorCode(t, err, "AccessDenied")
			}
			for _, versionID := range []*string{nil, version} {
				_, err = grantee.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
					Bucket: &bucket, Key: &key, VersionId: versionID,
					ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize},
				})
				requireS3ErrorCode(t, err, "AccessDenied")
			}
			_, err = grantee.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key, VersionId: version})
			requireS3ErrorCode(t, err, "AccessDenied")
			plainDeleteKey := "plain-delete-" + key
			matrixPutObject(t, owner, bucket, plainDeleteKey, payload)
			deleted, err := grantee.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &plainDeleteKey})
			if canWrite {
				if err != nil || !aws.ToBool(deleted.DeleteMarker) {
					t.Fatalf("shared ordinary delete = %#v, %v", deleted, err)
				}
			} else {
				requireS3ErrorCode(t, err, "AccessDenied")
			}

			uploadKey := "upload-" + key
			upload, err := owner.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: &uploadKey})
			if err != nil {
				t.Fatal(err)
			}
			list, listErr := grantee.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket})
			if canRead {
				if listErr != nil || !slices.ContainsFunc(list.Uploads, func(item types.MultipartUpload) bool {
					return aws.ToString(item.UploadId) == aws.ToString(upload.UploadId)
				}) {
					t.Fatalf("shared multipart listing = %#v, %v", list, listErr)
				}
			} else {
				requireS3ErrorCode(t, listErr, "AccessDenied")
			}
			_, err = grantee.ListParts(ctx, &s3.ListPartsInput{Bucket: &bucket, Key: &uploadKey, UploadId: upload.UploadId})
			requireS3ErrorCode(t, err, "AccessDenied")
			_, err = grantee.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &uploadKey, UploadId: upload.UploadId})
			requireS3ErrorCode(t, err, "AccessDenied")
			if _, err := owner.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &uploadKey, UploadId: upload.UploadId}); err != nil {
				t.Fatalf("owner abort after denied abort: %v", err)
			}
			ownUpload, err := grantee.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: &uploadKey})
			if canWrite {
				if err != nil {
					t.Fatal(err)
				}
				part, err := grantee.UploadPart(ctx, &s3.UploadPartInput{Bucket: &bucket, Key: &uploadKey, UploadId: ownUpload.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(payload)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := grantee.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket: &bucket, Key: &uploadKey, UploadId: ownUpload.UploadId,
					MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}},
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				requireS3ErrorCode(t, err, "AccessDenied")
			}
			for _, quiet := range []bool{false, true} {
				t.Run(fmt.Sprintf("DeleteObjects/quiet=%t", quiet), func(t *testing.T) {
					deleteKey := fmt.Sprintf("delete-%s-%t", key, quiet)
					matrixPutObject(t, owner, bucket, deleteKey, payload)
					batch, err := grantee.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &bucket, Delete: &types.Delete{
						Quiet: &quiet, Objects: []types.ObjectIdentifier{{Key: &deleteKey}, {Key: &key, VersionId: version}},
					}})
					wantDeleted, wantErrors := 0, 2
					if canWrite {
						wantErrors = 1
						if !quiet {
							wantDeleted = 1
						}
					}
					if err != nil || len(batch.Deleted) != wantDeleted || len(batch.Errors) != wantErrors {
						t.Fatalf("shared batch delete = %#v, %v", batch, err)
					}
					for _, denied := range batch.Errors {
						if aws.ToString(denied.Code) != "AccessDenied" || (aws.ToString(denied.Key) != deleteKey && aws.ToString(denied.Key) != key) {
							t.Fatalf("batch denial = %#v", denied)
						}
						if aws.ToString(denied.Key) == key && aws.ToString(denied.VersionId) != aws.ToString(version) {
							t.Fatalf("batch denial lost version: %#v", denied)
						}
					}
					_, headErr := owner.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &deleteKey})
					if canWrite {
						requireS3ErrorCode(t, headErr, "NotFound")
					} else if headErr != nil {
						t.Fatal(headErr)
					}
				})
			}
			for name, privileged := range map[string]*s3.Client{"owner": owner, "admin": s3Admin} {
				attributes, err := privileged.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
					Bucket: &bucket, Key: &key, VersionId: version,
					ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize},
				})
				if err != nil || aws.ToInt64(attributes.ObjectSize) != int64(len(payload)) {
					t.Fatalf("%s attributes after denied delete = %#v, %v", name, attributes, err)
				}
				deleteKey := name + "-" + key
				marker, err := owner.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &deleteKey})
				if err != nil || !aws.ToBool(marker.DeleteMarker) {
					t.Fatalf("create version-delete fixture = %#v, %v", marker, err)
				}
				if _, err := privileged.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &deleteKey, VersionId: marker.VersionId}); err != nil {
					t.Fatalf("%s version delete: %v", name, err)
				}
			}
		})
	}
}

func testMatrixDeleteValidation(t *testing.T, harness *systemtest.Harness, client *s3.Client) {
	bucket, key := "s3-matrix-delete-validation", "retained.bin"
	admin := e2e.NewAdminClient(t, harness.AdminURL)
	admin.Login(t, t.Context(), systemtest.AdminUsername, systemtest.AdminPassword)
	if _, err := client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatal(err)
	}
	waitForBucketReady(t, admin, bucket)
	version := matrixPutObject(t, client, bucket, key, bytes.Repeat([]byte("retained"), 32)).VersionId
	rawClient := matrixHTTPClient(t, harness.S3SocketPath())
	objectXML := "<Object><Key>" + key + "</Key></Object>"
	for _, tc := range []struct {
		name, code, body string
	}{
		{"empty", "MalformedXML", "<Delete></Delete>"},
		{"over limit", "MalformedXML", "<Delete>" + strings.Repeat(objectXML, 1001) + "</Delete>"},
		{"invalid key", "400", "<Delete>" + objectXML + "<Object><Key>../outside.bin</Key></Object></Delete>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://synaps3.system.test/"+bucket+"?delete", strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			sum := md5.Sum([]byte(tc.body))
			request.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
			matrixSignRequest(t, request, "UNSIGNED-PAYLOAD")
			response, err := rawClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			matrixRequireHTTPError(t, response, http.StatusBadRequest, tc.code)
			head, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
			if err != nil || aws.ToString(head.VersionId) != aws.ToString(version) {
				t.Fatalf("rejected batch changed current version: %#v, %v", head, err)
			}
		})
	}
}

func testMatrixContentEncoding(t *testing.T, harness *systemtest.Harness, client *s3.Client, bucket string) {
	rawClient := matrixHTTPClient(t, harness.S3SocketPath())
	plain := make([]byte, 256)
	for i := range plain {
		plain[i] = byte(i)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	const encoding = "aws-chunked,gzip"
	for _, method := range []string{http.MethodPut, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			key := "encoding-" + method + ".bin"
			var request *http.Request
			var err error
			if method == http.MethodPost {
				request = matrixPostRequest(t, bucket, key, compressed.Bytes(), map[string]string{"Content-Encoding": encoding})
			} else {
				request, err = http.NewRequestWithContext(t.Context(), method, "http://synaps3.system.test/"+bucket+"/"+key, bytes.NewReader(compressed.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Content-Encoding", encoding)
				sum := sha256.Sum256(compressed.Bytes())
				matrixSignRequest(t, request, hex.EncodeToString(sum[:]))
			}
			response, err := rawClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			responseBody := matrixReadResponse(t, response)
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				t.Fatalf("%s object = %d %s", method, response.StatusCode, responseBody)
			}
			matrixAssertEncodedObject(t, rawClient, client, bucket, key, encoding, compressed.Bytes())
		})
	}
}

func matrixAssertEncodedObject(t *testing.T, rawClient *http.Client, client *s3.Client, bucket, key, encoding string, payload []byte) {
	t.Helper()
	copyKey := "copied-" + key
	if _, err := client.CopyObject(t.Context(), &s3.CopyObjectInput{Bucket: &bucket, Key: &copyKey, CopySource: aws.String(bucket + "/" + key)}); err != nil {
		t.Fatal(err)
	}
	for _, objectKey := range []string{key, copyKey} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			request, err := http.NewRequestWithContext(t.Context(), method, "http://synaps3.system.test/"+bucket+"/"+objectKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			matrixSignRequest(t, request, "UNSIGNED-PAYLOAD")
			response, err := rawClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			got := matrixReadResponse(t, response)
			if response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != encoding || response.ContentLength != int64(len(payload)) {
				t.Fatalf("%s %s = %d, encoding %q, length %d", method, objectKey, response.StatusCode, response.Header.Get("Content-Encoding"), response.ContentLength)
			}
			if method == http.MethodGet && !bytes.Equal(got, payload) {
				t.Fatalf("GET %s content differs", objectKey)
			}
		}
	}
}

func matrixPostRequest(t *testing.T, bucket, key string, payload []byte, extraFields map[string]string) *http.Request {
	t.Helper()
	now := time.Now().UTC()
	date := now.Format("20060102")
	fields := map[string]string{
		"key": key, "x-amz-algorithm": "AWS4-HMAC-SHA256",
		"x-amz-credential": systemtest.OwnerAccess + "/" + date + "/us-east-1/s3/aws4_request",
		"x-amz-date":       now.Format("20060102T150405Z"),
	}
	maps.Copy(fields, extraFields)
	conditions := []any{map[string]string{"bucket": bucket}}
	for field, value := range fields {
		conditions = append(conditions, map[string]string{field: value})
	}
	policy, err := json.Marshal(map[string]any{"expiration": now.Add(time.Hour).Format(time.RFC3339), "conditions": conditions})
	if err != nil {
		t.Fatal(err)
	}
	fields["policy"] = base64.StdEncoding.EncodeToString(policy)
	signingKey := []byte("AWS4" + systemtest.OwnerSecret)
	for _, value := range []string{date, "us-east-1", "s3", "aws4_request"} {
		mac := hmac.New(sha256.New, signingKey)
		_, _ = mac.Write([]byte(value))
		signingKey = mac.Sum(nil)
	}
	mac := hmac.New(sha256.New, signingKey)
	_, _ = mac.Write([]byte(fields["policy"]))
	fields["x-amz-signature"] = hex.EncodeToString(mac.Sum(nil))
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for field, value := range fields {
		if err := form.WriteField(field, value); err != nil {
			t.Fatal(err)
		}
	}
	file, err := form.CreateFormFile("file", key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://synaps3.system.test/"+bucket, bytes.NewReader(body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", form.FormDataContentType())
	return request
}

func testMatrixUploadErrors(t *testing.T, harness *systemtest.Harness, client *s3.Client, bucket string) {
	rawClient := matrixHTTPClient(t, harness.S3SocketPath())
	payload := bytes.Repeat([]byte("upload body\n"), 512)
	t.Run("short Content-Length", func(t *testing.T) {
		payload := bytes.Repeat([]byte("upload body\n"), 2048)
		key := "short-content-length.bin"
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://synaps3.system.test/"+bucket+"/"+key, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.ContentLength = int64(len(payload) + 1)
		matrixSignRequest(t, request, "UNSIGNED-PAYLOAD")
		connection, err := net.DialTimeout("unix", harness.S3SocketPath(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = connection.Close() }()
		if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintf(connection, "PUT %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\n", request.URL.RequestURI(), request.URL.Host, request.ContentLength); err != nil {
			t.Fatal(err)
		}
		if err := request.Header.Write(connection); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Write(append([]byte("\r\n"), payload...)); err != nil {
			t.Fatal(err)
		}
		if err := connection.(*net.UnixConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(connection), request)
		if err != nil {
			t.Fatal(err)
		}
		matrixRequireHTTPError(t, response, http.StatusBadRequest, "IncompleteBody")
		matrixRequireNoVersions(t, client, bucket, key)
	})
	for _, tc := range []struct {
		name, body, code string
	}{
		{"invalid chunk", "not-hex\r\n", "IncompleteBody"},
		{"missing trailer", fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(payload), payload), "MalformedTrailerError"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := strings.ReplaceAll(tc.name, " ", "-") + ".bin"
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://synaps3.system.test/"+bucket+"/"+key, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Encoding", "aws-chunked")
			request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(payload)))
			request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
			matrixSignRequest(t, request, "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			response, err := rawClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			matrixRequireHTTPError(t, response, http.StatusBadRequest, tc.code)
			matrixRequireNoVersions(t, client, bucket, key)
		})
	}
	t.Run("early upload rejection", func(t *testing.T) {
		key := "denied-upload.bin"
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://synaps3.system.test/"+bucket+"/"+key, bytes.NewReader(bytes.Repeat([]byte("x"), 64<<10)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		if err := v4.NewSigner().SignHTTP(t.Context(), aws.Credentials{AccessKeyID: "UNKNOWNACCESS", SecretAccessKey: "unknown-secret"}, request, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		connection, err := net.DialTimeout("unix", harness.S3SocketPath(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = connection.Close() }()
		if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := request.Write(connection); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(connection)
		response, err := http.ReadResponse(reader, request)
		if err != nil {
			t.Fatal(err)
		}
		matrixRequireHTTPError(t, response, http.StatusForbidden, "InvalidAccessKeyId")
		next, err := http.NewRequestWithContext(t.Context(), http.MethodHead, "http://synaps3.system.test/"+bucket, nil)
		if err != nil {
			t.Fatal(err)
		}
		matrixSignRequest(t, next, "UNSIGNED-PAYLOAD")
		response, err = rawClient.Do(next)
		if err != nil {
			t.Fatalf("read after rejected upload: %v", err)
		}
		_ = matrixReadResponse(t, response)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("request after rejected upload = %d", response.StatusCode)
		}
		matrixRequireNoVersions(t, client, bucket, key)
	})
}

func testMatrixSSECustomerEncryption(t *testing.T, logger *slog.Logger) {
	serverTLS, clientTLS := matrixTLSConfig(t)
	harness, err := newSystemHarness(t, logger, systemtest.HarnessOptions{ServerTLS: serverTLS})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := harness.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	rawClient := matrixHTTPClient(t, harness.S3SocketPath())
	rawClient.Transport.(*http.Transport).TLSClientConfig = clientTLS
	client := s3.NewFromConfig(aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider(systemtest.OwnerAccess, systemtest.OwnerSecret, ""),
		HTTPClient: rawClient,
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String("https://synaps3.system.test")
		options.UsePathStyle = true
	})
	bucket, partKey := "s3-matrix-ssec", "multipart.bin"
	if _, err := client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatal(err)
	}
	admin := e2e.NewAdminClient(t, harness.AdminURL)
	admin.Login(t, t.Context(), systemtest.AdminUsername, systemtest.AdminPassword)
	waitForBucketReady(t, admin, bucket)
	payload := bytes.Repeat([]byte("SSE-C rejection\n"), 32)
	version := matrixPutObject(t, client, bucket, "retained.bin", payload).VersionId
	upload, err := client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: &partKey})
	if err != nil {
		t.Fatal(err)
	}
	part, err := client.UploadPart(t.Context(), &s3.UploadPartInput{Bucket: &bucket, Key: &partKey, UploadId: upload.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(payload)})
	if err != nil {
		t.Fatal(err)
	}
	keyBytes := bytes.Repeat([]byte("k"), 32)
	keyMD5 := md5.Sum(keyBytes)
	headers := map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       base64.StdEncoding.EncodeToString(keyBytes),
		"X-Amz-Server-Side-Encryption-Customer-Key-MD5":   base64.StdEncoding.EncodeToString(keyMD5[:]),
	}
	for _, tc := range []struct {
		name, method, path string
		invalidFields      bool
	}{
		{"PutObject", http.MethodPut, "/rejected.bin", false},
		{"CreateMultipartUpload", http.MethodPost, "/rejected-multipart.bin?uploads", false},
		{"UploadPart", http.MethodPut, "/" + partKey + "?partNumber=1&uploadId=" + url.QueryEscape(aws.ToString(upload.UploadId)), false},
		{"GetObject", http.MethodGet, "/retained.bin", false},
		{"ListParts invalid fields", http.MethodGet, "/" + partKey + "?uploadId=" + url.QueryEscape(aws.ToString(upload.UploadId)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, err := http.NewRequestWithContext(t.Context(), tc.method, "https://synaps3.system.test/"+bucket+tc.path, bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			for name, value := range headers {
				request.Header.Set(name, value)
			}
			if tc.invalidFields {
				request.Header.Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "invalid")
				request.Header.Set("X-Amz-Server-Side-Encryption-Customer-Key", "invalid")
			}
			matrixSignRequest(t, request, "UNSIGNED-PAYLOAD")
			response, err := rawClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			matrixRequireHTTPError(t, response, http.StatusNotImplemented, "NotImplemented")
		})
	}
	t.Run("PostObject", func(t *testing.T) {
		request := matrixPostRequest(t, bucket, "rejected-form.bin", payload, headers)
		request.URL.Scheme = "https"
		response, err := rawClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		matrixRequireHTTPError(t, response, http.StatusNotImplemented, "NotImplemented")
	})
	for _, key := range []string{"rejected.bin", "rejected-multipart.bin", "rejected-form.bin"} {
		matrixRequireNoVersions(t, client, bucket, key)
	}
	current, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: &bucket, Key: aws.String("retained.bin")})
	if err != nil || aws.ToString(current.VersionId) != aws.ToString(version) {
		t.Fatalf("SSE-C rejection changed object: %#v, %v", current, err)
	}
	parts, err := client.ListParts(t.Context(), &s3.ListPartsInput{Bucket: &bucket, Key: &partKey, UploadId: upload.UploadId})
	if err != nil || len(parts.Parts) != 1 || aws.ToString(parts.Parts[0].ETag) != aws.ToString(part.ETag) {
		t.Fatalf("SSE-C rejection changed multipart parts: %#v, %v", parts, err)
	}
	list, err := client.ListMultipartUploads(t.Context(), &s3.ListMultipartUploadsInput{Bucket: &bucket})
	if err != nil || len(list.Uploads) != 1 || aws.ToString(list.Uploads[0].UploadId) != aws.ToString(upload.UploadId) {
		t.Fatalf("SSE-C rejection changed multipart uploads: %#v, %v", list, err)
	}
}

func matrixTLSConfig(t *testing.T) (config.TLSConfig, *tls.Config) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "synaps3.system.test"},
		DNSNames: []string{"synaps3.system.test"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(t.TempDir(), "certificate.pem"), filepath.Join(t.TempDir(), "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("cannot trust generated certificate")
	}
	return config.TLSConfig{Enabled: true, CertFile: certFile, KeyFile: keyFile}, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

func matrixPutObject(t *testing.T, client *s3.Client, bucket, key string, body []byte) *s3.PutObjectOutput {
	t.Helper()
	out, err := client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func matrixHTTPClient(t *testing.T, socket string) *http.Client {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}
}

func matrixSignRequest(t *testing.T, request *http.Request, payloadHash string) {
	t.Helper()
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if err := v4.NewSigner().SignHTTP(request.Context(), aws.Credentials{AccessKeyID: systemtest.OwnerAccess, SecretAccessKey: systemtest.OwnerSecret}, request, payloadHash, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
}

func matrixReadResponse(t *testing.T, response *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func matrixRequireHTTPError(t *testing.T, response *http.Response, status int, code string) {
	t.Helper()
	body := matrixReadResponse(t, response)
	var result struct{ Code string }
	if err := xml.Unmarshal(body, &result); err != nil || response.StatusCode != status || result.Code != code {
		t.Fatalf("HTTP error = %d %s, XML error %v; want %d %s", response.StatusCode, body, err, status, code)
	}
}

func matrixRequireNoVersions(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	out, err := client.ListObjectVersions(t.Context(), &s3.ListObjectVersionsInput{Bucket: &bucket, Prefix: &key})
	if err != nil || len(out.Versions) != 0 || len(out.DeleteMarkers) != 0 {
		t.Fatalf("rejected upload left versions: %#v, %v", out, err)
	}
}
