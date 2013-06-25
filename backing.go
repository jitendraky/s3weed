package s3weed

/*
Copyright 2013 Tamás Gulácsi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import (
	"errors"
	"io"
	"time"
)

var backing WeedBacker

// NotFound prints Not Found
var NotFound = errors.New("Not Found")

// Bucket is a holder for objects
type Bucket struct {
	Name    string
	Created time.Time
}

// Object represents a file
type Object struct {
	Key          string
	LastModified time.Time
	Size         int64
	Owner        Owner
}

// Owner is the object's owner
type Owner interface {
	// ID returns the ID of this owner
	ID() string
	// Name returns then name of this owner
	Name() string
	// Sign HMAC-SHA1 signes the UTF-8 encoded string with the secret access key
	Sign(stringToSign []byte) []byte
}

// WeedBacker is an interface for what is needed for S3
type WeedBacker interface {
	// Authenticate checks authorization and returns an Owner or an error
	Authenticate(auth, secret string) (Owner, error)
	// ListBuckets list all buckets owned by the given owner
	ListBuckets(owner Owner) (Owner, []Bucket, error)
	// CreateBucket creates a new bucket
	CreateBucket(owner Owner, bucket string) error
	// CheckBucket returns whether the owner has a bucket named as given
	CheckBucket(owner Owner, bucket string) bool
	// DelBucket deletes a bucket
	DelBucket(owner Owner, bucket string) error
	// List lists a bucket, all objects Key starts with prefix, delimiter segments
	// Key, thus the returned commonprefixes (think a generalized filepath
	// structure, where / is the delimiter, a commonprefix is a subdir)
	List(owner Owner, bucket, prefix, delimiter, marker string, limit int) (
		objects []Object, commonprefixes []string, truncated bool, err error)
	// Put puts a file as a new object into the bucket
	Put(owner Owner, bucket, object, filename, media string, body io.Reader) error
	// Get retrieves an object from the bucket
	Get(owner Owner, bucket, object string) (filename, media string, body io.Reader, err error)
	// Del deletes the object from the bucket
	Del(owner Owner, bucket, object string) error
	// GetOwner returns the Owner for the accessKey - or an error
	GetOwner(accessKey string) (Owner, error)
}
