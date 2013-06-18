package s3weed

import (
	"errors"
	"io"
	"time"
)

var backing WeedBacker

var NotFound = errors.New("Not Found")

type Owner struct {
	Id   string
	Name string
}

type Bucket struct {
	Name    string
	Created time.Time
}

type Object struct {
	Key          string
	LastModified time.Time
	Size         int64
	Owner        Owner
}

type WeedBacker interface {
	ListBuckets(owner Owner) (Owner, []Bucket, error)
	CreateBucket(owner Owner, bucket string) error
	CheckBucket(owner Owner, bucket string) bool
	DelBucket(owner Owner, bucket string) error
	List(owner Owner, bucket, prefix, delimiter, marker string, limit int) (
		objects []Object, commonprefixes []string, truncated bool, err error)
	Put(owner Owner, bucket, object, filename, media string, body io.Reader) error
	Get(owner Owner, bucket, object string) (filename, media string, body io.Reader, err error)
	Del(owner Owner, bucket, object string) error
}
