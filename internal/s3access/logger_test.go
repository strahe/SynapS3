package s3access_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/strahe/synaps3/internal/s3access"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3log"
)

func TestLoggerLogsStructuredSuccessfulRequest(t *testing.T) {
	record := captureRequestLog(t, http.MethodPut, "/test1/Pencil-mac-arm64.dmg?x-id=UploadPart&partNumber=1&uploadId=u1", nil, []byte("response"), s3log.LogMeta{
		Action:     "UploadPart",
		ObjectSize: 12,
	})

	assertOnlyLogFields(t, record, "time", "level", "msg", "component", "operation", "method", "path", "status", "duration_ms")
	assertLogField(t, record, "msg", "s3 request completed")
	assertLogField(t, record, "level", "INFO")
	assertLogField(t, record, "component", "s3")
	assertLogField(t, record, "operation", "UploadPart")
	assertLogField(t, record, "method", http.MethodPut)
	assertLogField(t, record, "path", "/test1/Pencil-mac-arm64.dmg")
	assertLogNumber(t, record, "status", http.StatusOK)
	assertLogNumberRange(t, record, "duration_ms", 1500, 5000)
}

func TestLoggerOmitsQueryAndObjectDetailFields(t *testing.T) {
	record := captureRequestLog(t, http.MethodGet, "/bucket/key?X-Amz-Signature=secret&X-Amz-Credential=cred&AWSAccessKeyId=access&prefix=photos", nil, nil, s3log.LogMeta{
		Action: "GetObject",
	})

	for _, field := range []string{"query", "bucket", "key", "remote_ip", "bytes_sent", "object_size", "uploadID", "partNumber", "versionID", "bucket_owner"} {
		if _, ok := record[field]; ok {
			t.Fatalf("record should omit %q: %#v", field, record)
		}
	}
}

func TestLoggerLogsS3ErrorStatusAndCode(t *testing.T) {
	tests := []struct {
		name string
		err  s3err.S3Error
	}{
		{
			name: "api error",
			err:  s3err.GetAPIError(s3err.ErrNoSuchBucket),
		},
		{
			name: "typed invalid argument error",
			err:  s3err.GetInvalidArgumentErr(s3err.InvalidArgPartNumber, "0"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := captureRequestLog(t, http.MethodGet, "/missing", tt.err, nil, s3log.LogMeta{
				Action: "HeadBucket",
			})

			baseErr := tt.err.BaseError()
			assertOnlyLogFields(t, record, "time", "level", "msg", "component", "operation", "method", "path", "status", "duration_ms", "error", "error_code")
			assertLogNumber(t, record, "status", baseErr.HTTPStatusCode)
			assertLogField(t, record, "error_code", baseErr.Code)
			assertLogField(t, record, "error", tt.err.Error())
		})
	}
}

func TestLoggerLogsPlainErrorAsInternalServerError(t *testing.T) {
	logErr := errors.New("marshal response")
	record := captureRequestLog(t, http.MethodGet, "/bucket/key", logErr, nil, s3log.LogMeta{
		Action: "GetObject",
	})

	assertOnlyLogFields(t, record, "time", "level", "msg", "component", "operation", "method", "path", "status", "duration_ms", "error")
	assertLogNumber(t, record, "status", http.StatusInternalServerError)
	assertLogField(t, record, "error", logErr.Error())
}

func TestLoggerUsesConfiguredLevel(t *testing.T) {
	record := captureRequestLogAtLevel(t, "debug", http.MethodGet, "/bucket/key", nil, nil, s3log.LogMeta{
		Action: "GetObject",
	})

	assertLogField(t, record, "level", "DEBUG")
}

func captureRequestLog(t *testing.T, method, target string, logErr error, body []byte, meta s3log.LogMeta) map[string]any {
	t.Helper()
	return captureRequestLogAtLevel(t, "info", method, target, logErr, body, meta)
}

func captureRequestLogAtLevel(t *testing.T, level, method, target string, logErr error, body []byte, meta s3log.LogMeta) map[string]any {
	t.Helper()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auditLogger := s3access.NewLogger(logger, level)
	start := time.Now().Add(-1500 * time.Millisecond)

	app := fiber.New()
	app.All("/*", func(ctx *fiber.Ctx) error {
		utils.ContextKeyStartTime.Set(ctx, start)
		auditLogger.Log(ctx, logErr, body, meta)
		return ctx.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(method, target, nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	_ = resp.Body.Close()

	if strings.TrimSpace(buf.String()) == "" {
		t.Fatal("logger produced no output")
	}
	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("Unmarshal log %q: %v", buf.String(), err)
	}
	return record
}

func assertLogField(t *testing.T, record map[string]any, field string, want string) {
	t.Helper()
	if got, ok := record[field].(string); !ok || got != want {
		t.Fatalf("record[%q] = %#v, want %q", field, record[field], want)
	}
}

func assertOnlyLogFields(t *testing.T, record map[string]any, fields ...string) {
	t.Helper()
	allowed := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		allowed[field] = struct{}{}
	}
	for field := range record {
		if _, ok := allowed[field]; !ok {
			t.Fatalf("record includes unexpected field %q: %#v", field, record)
		}
	}
}

func assertLogNumber(t *testing.T, record map[string]any, field string, want int) {
	t.Helper()
	got, ok := record[field].(float64)
	if !ok {
		t.Fatalf("record[%q] = %#v, want numeric %d", field, record[field], want)
	}
	if int(got) != want {
		t.Fatalf("record[%q] = %v, want %d", field, got, want)
	}
}

func assertLogNumberRange(t *testing.T, record map[string]any, field string, min, max int) {
	t.Helper()
	got, ok := record[field].(float64)
	if !ok {
		t.Fatalf("record[%q] = %#v, want numeric value in [%d, %d]", field, record[field], min, max)
	}
	gotInt := int(got)
	if gotInt < min || gotInt > max {
		t.Fatalf("record[%q] = %v, want value in [%d, %d]", field, got, min, max)
	}
}
