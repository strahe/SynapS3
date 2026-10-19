package backend_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func TestSSECustomerKeysRejectedBeforeStorageAccess(t *testing.T) {
	b := &backend.SynapseBackend{}
	ctx := t.Context()
	bucket, key, uploadID, source := "ssec-bucket", "object", "upload", "ssec-bucket/source"
	partNumber := int32(1)

	operations := []struct {
		name string
		run  func(algorithm, customerKey, keyMD5 *string) error
	}{
		{name: "PutObject", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.PutObject(ctx, s3response.PutObjectInput{
				Bucket: &bucket, Key: &key,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "GetObject", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &bucket, Key: &key,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "HeadObject", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: &bucket, Key: &key,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "GetObjectAttributes", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
				Bucket: &bucket, Key: &key,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "CopyObject destination", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.CopyObject(ctx, s3response.CopyObjectInput{
				Bucket: &bucket, Key: &key, CopySource: &source,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "CopyObject source", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.CopyObject(ctx, s3response.CopyObjectInput{
				Bucket: &bucket, Key: &key, CopySource: &source,
				CopySourceSSECustomerAlgorithm: algorithm, CopySourceSSECustomerKey: customerKey, CopySourceSSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "CreateMultipartUpload", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
				Bucket: &bucket, Key: &key,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "UploadPart", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &partNumber,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "UploadPartCopy destination", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
				Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &partNumber, CopySource: &source,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "UploadPartCopy source", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
				Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &partNumber, CopySource: &source,
				CopySourceSSECustomerAlgorithm: algorithm, CopySourceSSECustomerKey: customerKey, CopySourceSSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "CompleteMultipartUpload", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, _, err := b.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket: &bucket, Key: &key, UploadId: &uploadID,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{{PartNumber: &partNumber, ETag: aws.String("etag")}},
				},
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
		{name: "ListParts", run: func(algorithm, customerKey, keyMD5 *string) error {
			_, err := b.ListParts(ctx, &s3.ListPartsInput{
				Bucket: &bucket, Key: &key, UploadId: &uploadID,
				SSECustomerAlgorithm: algorithm, SSECustomerKey: customerKey, SSECustomerKeyMD5: keyMD5,
			})
			return err
		}},
	}
	for _, operation := range operations {
		for _, field := range []struct {
			name                        string
			algorithm, customerKey, md5 *string
		}{
			{name: "algorithm", algorithm: aws.String("AES256")},
			{name: "key", customerKey: aws.String("customer-key")},
			{name: "key MD5", md5: aws.String("key-md5")},
		} {
			t.Run(operation.name+"/"+field.name, func(t *testing.T) {
				err := operation.run(field.algorithm, field.customerKey, field.md5)
				requireAPIErrorCode(t, err, s3err.GetAPIError(s3err.ErrNotImplemented))
			})
		}
	}
}
