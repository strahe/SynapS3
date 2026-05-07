package s3access

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3log"
)

type Logger struct {
	logger *slog.Logger
	level  slog.Level
}

var _ s3log.AuditLogger = (*Logger)(nil)

func NewLogger(logger *slog.Logger, level string) *Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &Logger{logger: logger, level: parseLevel(level)}
}

func (l *Logger) Log(ctx *fiber.Ctx, err error, _ []byte, meta s3log.LogMeta) {
	if ctx == nil {
		return
	}
	attrs := []any{
		"component", "s3",
		"operation", meta.Action,
		"method", ctx.Method(),
		"path", ctx.Path(),
		"status", statusCode(ctx, err, meta),
		"duration_ms", durationMillis(ctx),
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
		var apiErr s3err.APIError
		if errors.As(err, &apiErr) {
			attrs = append(attrs, "error_code", apiErr.Code)
		}
	}
	l.logger.Log(ctx.UserContext(), l.level, "s3 request completed", attrs...)
}

func (l *Logger) HangUp() error {
	return nil
}

func (l *Logger) Shutdown() error {
	return nil
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func statusCode(ctx *fiber.Ctx, err error, meta s3log.LogMeta) int {
	var apiErr s3err.APIError
	if err != nil && errors.As(err, &apiErr) {
		return apiErr.HTTPStatusCode
	}
	if meta.HttpStatus != 0 {
		return meta.HttpStatus
	}
	if err != nil {
		return http.StatusInternalServerError
	}
	if status := ctx.Response().StatusCode(); status != 0 {
		return status
	}
	return http.StatusOK
}

func durationMillis(ctx *fiber.Ctx) int64 {
	start, ok := utils.ContextKeyStartTime.Get(ctx).(time.Time)
	if !ok || start.IsZero() {
		return 0
	}
	return time.Since(start).Milliseconds()
}
