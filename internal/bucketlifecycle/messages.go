package bucketlifecycle

const MessageBucketReady = "bucket.ready"

type BucketReady struct{ BucketID int64 }

func (BucketReady) MessageType() string { return MessageBucketReady }
