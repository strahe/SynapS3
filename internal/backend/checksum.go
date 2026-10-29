package backend

import (
	"encoding/base64"
	"encoding/hex"
	"io"

	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3api/utils"
)

type writeChecksums struct {
	CRC32, CRC32C, SHA1, SHA256, CRC64NVME, SHA512, MD5, XXHASH64, XXHASH3, XXHASH128 *string
}

func withWriteChecksum(body io.Reader, checksums writeChecksums) (io.Reader, error) {
	if reader, ok := body.(middlewares.ChecksumReader); ok && reader.Algorithm() != "" {
		return body, nil
	}
	for _, header := range []struct {
		value    *string
		hashType utils.HashType
	}{
		{checksums.CRC32, utils.HashTypeCRC32},
		{checksums.CRC32C, utils.HashTypeCRC32C},
		{checksums.SHA1, utils.HashTypeSha1},
		{checksums.SHA256, utils.HashTypeSha256},
		{checksums.CRC64NVME, utils.HashTypeCRC64NVME},
		{checksums.SHA512, utils.HashTypeSha512},
		{checksums.MD5, utils.HashTypeMd5},
		{checksums.XXHASH64, utils.HashTypeXXHASH64},
		{checksums.XXHASH3, utils.HashTypeXXHASH3},
		{checksums.XXHASH128, utils.HashTypeXXHASH128},
	} {
		if header.value != nil && *header.value != "" {
			return utils.NewHashReader(body, *header.value, header.hashType)
		}
	}
	return body, nil
}

func base64Checksum(checksum string) (string, error) {
	decoded, err := hex.DecodeString(checksum)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(decoded), nil
}
