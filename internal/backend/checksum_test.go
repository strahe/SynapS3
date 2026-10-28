package backend

import (
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
)

func TestWriteChecksumValidatesEachHeaderAlgorithm(t *testing.T) {
	for _, tc := range []struct {
		algorithm utils.HashType
		set       func(*writeChecksums, *string)
	}{
		{utils.HashTypeCRC32, func(c *writeChecksums, sum *string) { c.CRC32 = sum }},
		{utils.HashTypeCRC32C, func(c *writeChecksums, sum *string) { c.CRC32C = sum }},
		{utils.HashTypeSha1, func(c *writeChecksums, sum *string) { c.SHA1 = sum }},
		{utils.HashTypeSha256, func(c *writeChecksums, sum *string) { c.SHA256 = sum }},
		{utils.HashTypeCRC64NVME, func(c *writeChecksums, sum *string) { c.CRC64NVME = sum }},
		{utils.HashTypeSha512, func(c *writeChecksums, sum *string) { c.SHA512 = sum }},
		{utils.HashTypeMd5, func(c *writeChecksums, sum *string) { c.MD5 = sum }},
		{utils.HashTypeXXHASH64, func(c *writeChecksums, sum *string) { c.XXHASH64 = sum }},
		{utils.HashTypeXXHASH3, func(c *writeChecksums, sum *string) { c.XXHASH3 = sum }},
		{utils.HashTypeXXHASH128, func(c *writeChecksums, sum *string) { c.XXHASH128 = sum }},
	} {
		t.Run(string(tc.algorithm), func(t *testing.T) {
			for _, matches := range []bool{true, false} {
				hash, err := utils.NewHash(tc.algorithm)
				if err != nil {
					t.Fatal(err)
				}
				if matches {
					_, _ = io.WriteString(hash, "object bytes")
				}
				sum := base64.StdEncoding.EncodeToString(hash.Sum(nil))
				var checksums writeChecksums
				tc.set(&checksums, &sum)
				body, err := withWriteChecksum(strings.NewReader("object bytes"), checksums)
				if err != nil {
					t.Fatal(err)
				}
				_, err = io.ReadAll(body)
				if matches {
					if err != nil {
						t.Fatalf("matching checksum rejected: %v", err)
					}
				} else if apiErr, ok := errors.AsType[s3err.S3Error](err); !ok || apiErr.BaseError().Code != "BadDigest" {
					t.Fatalf("mismatched checksum error = %v; want BadDigest", err)
				}
			}
		})
	}
}

type declaredTrailerChecksumReader struct{ io.Reader }

func (*declaredTrailerChecksumReader) Algorithm() string { return "sha256" }

func (*declaredTrailerChecksumReader) Checksum() string { return "" }

func TestWriteChecksumPreservesTrailerReaderAndContentMD5Validation(t *testing.T) {
	trailer := &declaredTrailerChecksumReader{Reader: strings.NewReader("body")}
	wrong := "wrong-checksum"
	wrapped, err := withWriteChecksum(trailer, writeChecksums{SHA256: &wrong})
	if err != nil || wrapped != trailer {
		t.Fatalf("declared trailer reader = %T, %v; want the original reader", wrapped, err)
	}
	md5Reader, err := utils.NewHashReader(strings.NewReader("body"), "1B2M2Y8AsgTpgAmY7PhCfg==", utils.HashTypeContentMD5)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err = withWriteChecksum(md5Reader, writeChecksums{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(wrapped)
	if apiErr, ok := errors.AsType[s3err.S3Error](err); !ok || apiErr.BaseError().Code != "BadDigest" {
		t.Fatalf("Content-MD5 error = %v; want BadDigest", err)
	}
}
