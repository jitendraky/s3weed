/*
Package s3intf defines an interface for an S3 server

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
package s3intf

import (
	"hash"
	"io"
	"time"
)

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
	// GetHMAC returns a HMAC initialized with the secret key
	GetHMAC(h func() hash.Hash) hash.Hash
}

// Backer is an interface for what is needed for S3
// You must implement this, and than s3srv can use this Backer to implement
// the server
type Backer interface {
	// ListBuckets list all buckets owned by the given owner
	ListBuckets(owner Owner) ([]Bucket, error)
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
	Get(owner Owner, bucket, object string) (filename, media string, body io.ReadCloser, err error)
	// Del deletes the object from the bucket
	Del(owner Owner, bucket, object string) error
	// GetOwner returns the Owner for the accessKey - or an error
	GetOwner(accessKey string) (Owner, error)
}
