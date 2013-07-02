/*
Package dirS3 program implements s3intf.Backer as a simple directory hierarchy
This is NOT for production use, only an experiment for testing the usability
of the s3intf API!

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
package dirS3

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/tgulacsi/s3weed/s3intf"
	"hash"
	"io"
	//"log"
	"os"
	"path/filepath"
	"strings"
)

type hier string

// NewDirS3 stores everything under a common root.
// The first level of subdirs are the owners,
// the second are the buckets, and each object is a file under the bucket dir.
func NewDirS3(root string) s3intf.Storage {
	os.MkdirAll(root, 0750)
	return hier(root)
}

// ListBuckets list all buckets owned by the given owner
func (root hier) ListBuckets(owner s3intf.Owner) ([]s3intf.Bucket, error) {
	dh, err := os.Open(filepath.Join(string(root), owner.ID()))
	if err != nil {
		return nil, err
	}
	defer dh.Close()
	infos, err := dh.Readdir(1000)
	if err != nil {
		return nil, err
	}
	buckets := make([]s3intf.Bucket, len(infos))
	for i, fi := range infos {
		buckets[i].Name = fi.Name()
		buckets[i].Created = fi.ModTime()
	}
	return buckets, nil
}

// CreateBucket creates a new bucket
func (root hier) CreateBucket(owner s3intf.Owner, bucket string) error {
	return os.MkdirAll(filepath.Join(string(root), owner.ID(), bucket), 0750)
}

// CheckBucket returns whether the owner has a bucket named as given
func (root hier) CheckBucket(owner s3intf.Owner, bucket string) bool {
	dh, err := os.Open(filepath.Join(string(root), owner.ID(), bucket))
	if err != nil {
		return false
	}
	dh.Close()
	return true
}

// DelBucket deletes a bucket
func (root hier) DelBucket(owner s3intf.Owner, bucket string) error {
	dh, err := os.Open(filepath.Join(string(root), owner.ID(), bucket))
	if err != nil {
		return err
	}
	infos, err := dh.Readdir(1)
	if err != nil {
		dh.Close()
		return err
	}
	if len(infos) > 0 {
		dh.Close()
		return errors.New("cannot delete non-empty bucket")
	}
	nm := dh.Name()
	dh.Close()
	return os.Remove(nm)
}

// List lists a bucket, all objects Key starts with prefix, delimiter segments
// Key, thus the returned commonprefixes (think a generalized filepath
// structure, where / is the delimiter, a commonprefix is a subdir)
func (root hier) List(owner s3intf.Owner, bucket, prefix, delimiter, marker string, limit int) (
	objects []s3intf.Object, commonprefixes []string, truncated bool, err error) {
	return
}

// Put puts a file as a new object into the bucket
func (root hier) Put(owner s3intf.Owner, bucket, object, filename, media string, body io.Reader) error {
	fh, err := os.Create(filepath.Join(string(root), owner.ID(), bucket,
		encodeFilename(object, filename, media)))
	if err != nil {
		return err
	}
	_, err = io.Copy(fh, body)
	fh.Close()
	return err
}

var b64 = base64.URLEncoding

func encodeFilename(parts ...string) string {
	for i, s := range parts {
		parts[i] = b64.EncodeToString([]byte(s))
	}
	return strings.Join(parts, "#")
}

func decodeFilename(fn string) (object, filename, media string, err error) {
	strs := strings.SplitN(fn, "#", 3)
	var b []byte
	if b, err = b64.DecodeString(strs[0]); err != nil {
		return
	}
	object = string(b)
	if b, err = b64.DecodeString(strs[1]); err != nil {
		return
	}
	filename = string(b)
	if b, err = b64.DecodeString(strs[2]); err != nil {
		return
	}
	media = string(b)
	return
}

func (root hier) findFile(owner s3intf.Owner, bucket, object string) (string, error) {
	dh, err := os.Open(filepath.Join(string(root), owner.ID(), bucket))
	if err != nil {
		return "", err
	}
	defer dh.Close()
	prefix := encodeFilename(object)
	var names []string
	for err == nil {
		if names, err = dh.Readdirnames(1000); err != nil {
			return "", err
		}
		for _, nm := range names {
			if strings.HasPrefix(nm, prefix) {
				return filepath.Join(dh.Name(), nm), nil
			}
		}
	}
	return "", nil
}

// Get retrieves an object from the bucket
func (root hier) Get(owner s3intf.Owner, bucket, object string) (
	filename, media string, body io.ReadCloser, err error) {
	fn, e := root.findFile(owner, bucket, object)
	if e != nil {
		err = e
		return
	}
	if fn == "" {
		err = fmt.Errorf("no such file as %s: %s", fn, e)
		return
	}
	body, e = os.Open(fn)
	if e != nil {
		err = e
		return
	}
	_, filename, media, err = decodeFilename(filepath.Base(fn))
	return
}

// Del deletes the object from the bucket
func (root hier) Del(owner s3intf.Owner, bucket, object string) error {
	fn, err := root.findFile(owner, bucket, object)
	if err != nil {
		return err
	}
	return os.Remove(fn)
}

type user string

// ID returns the ID of this owner
func (u user) ID() string {
	return string(u)
}

// Name returns then name of this owner
func (u user) Name() string {
	return string(u)
}

// GetHMAC returns a HMAC initialized with the secret key
func (u user) GetHMAC(h func() hash.Hash) hash.Hash {
	return hmac.New(h, nil)
}

// Check checks the validity of the authorization
func (u user) CalcHash(bytesToSign []byte) []byte {
	return s3intf.CalcHash(hmac.New(sha1.New, nil), bytesToSign)
}

// GetOwner returns the Owner for the accessKey - or an error
func (root hier) GetOwner(accessKey string) (s3intf.Owner, error) {
	return user(accessKey), nil
}
