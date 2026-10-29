package backend

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	versitybackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
)

func normalizedETag(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.Trim(*value, `"`)
	return &trimmed
}

func writeObjectCondition(ifMatch, ifNoneMatch *string) repository.ObjectCondition {
	if ifMatch == nil && ifNoneMatch == nil {
		return nil
	}
	ifMatch, ifNoneMatch = normalizedETag(ifMatch), normalizedETag(ifNoneMatch)
	return func(version *model.ObjectVersion) error {
		exists := version != nil && !version.IsDeleteMarker
		etag := ""
		if exists {
			etag = version.ETag
		}
		return versitybackend.EvaluateObjectPutPreconditions(etag, ifMatch, ifNoneMatch, exists)
	}
}

func evaluateReadConditions(etag string, modified time.Time, conditions versitybackend.PreConditions, copySource bool) error {
	modified = modified.Truncate(time.Second)
	if conditions.IfMatch != nil {
		if !readETagMatches(etag, *conditions.IfMatch, true) {
			return s3err.GetPreconditionFailedErr(s3err.ConditionIfMatch)
		}
		conditions.IfMatch = nil
		conditions.IfUnmodeSince = nil
	}
	hasIfNoneMatch := conditions.IfNoneMatch != nil
	noneMatch := false
	if conditions.IfNoneMatch != nil {
		noneMatch = readETagMatches(etag, *conditions.IfNoneMatch, false)
		conditions.IfNoneMatch = nil
		conditions.IfModSince = nil
	}
	if conditions.IfUnmodeSince != nil {
		// HTTP dates include their equality boundary; the gateway helper uses After.
		inclusive := conditions.IfUnmodeSince.Truncate(time.Second).Add(time.Nanosecond)
		conditions.IfUnmodeSince = &inclusive
	}
	err := versitybackend.EvaluatePreconditions(etag, modified, conditions)
	if err == nil && noneMatch {
		err = s3err.GetAPIError(s3err.ErrNotModified)
	}
	if copySource && err != nil {
		var s3Error s3err.S3Error
		if errors.As(err, &s3Error) && s3Error.BaseError().HTTPStatusCode == http.StatusNotModified {
			if hasIfNoneMatch {
				return s3err.GetPreconditionFailedErr(s3err.ConditionIfNoneMatch)
			}
			return s3err.GetAPIError(s3err.ErrPreconditionFailed)
		}
	}
	return err
}

func readETagMatches(etag, value string, strong bool) bool {
	etag = strings.Trim(etag, `"`)
	if matches, valid := readETagListMatches(etag, value, strong); valid {
		return matches
	}
	// The gateway strips a surrounding quote pair from the entire header value.
	matches, _ := readETagListMatches(etag, `"`+value+`"`, strong)
	return matches
}

func readETagListMatches(etag, value string, strong bool) (bool, bool) {
	value = strings.Trim(value, " \t")
	if value == "*" {
		return true, true
	}
	matches := false
	for {
		value = strings.TrimLeft(value, " \t,")
		if value == "" {
			return matches, true
		}
		weak := strings.HasPrefix(value, "W/")
		if weak {
			value = value[2:]
		}
		if len(value) < 2 || value[0] != '"' {
			return false, false
		}
		end := strings.IndexByte(value[1:], '"') + 1
		if end == 0 {
			return false, false
		}
		for i := 1; i < end; i++ {
			if value[i] < 0x21 || value[i] == 0x7f {
				return false, false
			}
		}
		matches = matches || ((!strong || !weak) && value[1:end] == etag)
		value = strings.TrimLeft(value[end+1:], " \t")
		if value != "" && value[0] != ',' {
			return false, false
		}
	}
}

func deleteObjectCondition(conditions versitybackend.ObjectDeletePreconditions) repository.ObjectCondition {
	if !conditions.IsSet() {
		return nil
	}
	return func(version *model.ObjectVersion) error {
		if version == nil {
			return s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		if version.IsDeleteMarker {
			if conditions.IfMatch != nil {
				return s3err.GetPreconditionFailedErr(s3err.ConditionIfMatch)
			}
			return s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return versitybackend.EvaluateObjectDeletePreconditions(version.ETag, version.CreatedAt, version.Size, conditions)
	}
}
