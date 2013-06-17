package s3weed

import (
	"errors"
	"io"
	//"time"
)

var backing WeedBacker

var NotFound = errors.New("Not Found")

type Owner struct {
	Id   string
	Name string
}

type Object struct {
	Key  string
	Size int64
	Owner
}

type WeedBacker interface {
	ListBuckets() (Owner, []string, error)
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListObjects(bucket, prefix, delimiter, marker string, limit int) (
		objects []Object, commonprefixes []string, truncated bool, err error)
	PutObject(bucket, object, filename, media string, body io.Reader) error
	GetObject(bucket, object string) (filename, media string, body io.Reader, err error)
	DeleteObject(bucket, object string) error
}
