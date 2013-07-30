/*
Package weedS3 program implements s3intf.Backer as a simple directory hierarchy
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
package weedS3

import (
	"github.com/cznic/kv"
	"github.com/tgulacsi/s3weed/s3intf"

	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"io"
	//"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type wBucket struct {
	filename string
	created  time.Time
	db       *kv.DB
}

type wOwner struct {
	dir     string
	buckets map[string]wBucket
	sync.Mutex
}

type master struct {
	weedMaster string // master weed node's URL
	baseDir    string
	owners     map[string]wOwner
	sync.Mutex
}

// NewWeedS3 stores everything in the given master Weed-FS node
// buckets are stored
func NewWeedS3(masterURL, dbdir string) (s3intf.Storage, error) {
	m := master{weedMaster: masterURL, baseDir: dbdir, owners: nil}
	dh, err := os.Open(dbdir)
	if err != nil {
		if _, ok := err.(*os.PathError); !ok {
			return nil, err
		}
		os.MkdirAll(dbdir, 0750)
		m.owners = make(map[string]wOwner, 2)
	}
	defer dh.Close()
	var nm string
	var fi os.FileInfo
	for {
		fis, err := dh.Readdir(1000)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if m.owners == nil {
			m.owners = make(map[string]wOwner, len(fis))
		}
		for _, fi = range fis {
			if !fi.IsDir() {
				continue
			}
			nm = fi.Name()
			if m.owners[nm], err = openOwner(filepath.Join(dbdir, nm)); err != nil {
				return nil, err
			}
		}
	}
	return m, nil
}

func openOwner(dir string) (o wOwner, err error) {
	dh, e := os.Open(dir)
	if e != nil {
		err = e
		return
	}
	defer dh.Close()
	//o = wOwner{dir: dir, buckets: nil}
	o.dir = dir
	var (
		k, nm string
		db    *kv.DB
		fis   []os.FileInfo
	)
	for {
		if fis, e = dh.Readdir(1000); e != nil {
			if e == io.EOF {
				break
			}
			err = e
			return
		}
		if o.buckets == nil {
			o.buckets = make(map[string]wBucket, len(fis))
		}
		for _, fi := range fis {
			nm = fi.Name()
			if !(strings.HasSuffix(nm, ".kv") && len(nm) > 3) {
				continue
			}
			k = nm[:len(nm)-3]
			if o.buckets[k], err = openBucket(filepath.Join(dir, nm)); err != nil {
				return
			}
		}
	}

	return
}

func openBucket(filename string) (b wBucket, err error) {
	fh, e := os.Open(filename)
	if e != nil {
		err = e
		return
	}
	fi, e := fh.Stat()
	fh.Close()
	if e != nil {
		err = e
		return
	}
	//b = bucket{filename: filename, created: fi.ModTime()}
	b.filename, b.created = filename, fi.ModTime()
	b.db, err = kv.Open(filename, nil)
	if err != nil {
		err = fmt.Errorf("error opening buckets db %s: %s", filename, err)
		return
	}
	return
}

// ListBuckets list all buckets owned by the given owner
func (m master) ListBuckets(owner s3intf.Owner) ([]s3intf.Bucket, error) {
	m.Lock()
	o, ok := m.owners[owner.ID()]
	m.Unlock()
	if !ok {
		return nil, fmt.Errorf("unkown owner %s", owner.ID())
	}
	buckets := make([]s3intf.Bucket, len(o.buckets))
	i := 0
	for k, b := range o.buckets {
		buckets[i].Name = k
		buckets[i].Created = b.created
		i++
	}
	return buckets, nil
}

// CreateBucket creates a new bucket
func (m master) CreateBucket(owner s3intf.Owner, bucket string) error {
	m.Lock()
	defer m.Unlock()
	o, ok := m.owners[owner.ID()]
	if !ok {
		dir := filepath.Join(m.baseDir, owner.ID())
		err := os.MkdirAll(dir, 0750)
		if err != nil {
			return err
		}
		o = wOwner{dir: dir, buckets: make(map[string]wBucket, 1)}
		m.owners[owner.ID()] = o
	}
	o.Lock()
	defer o.Unlock()
	_, ok = o.buckets[bucket]
	if ok {
		return nil //AlreadyExists ?
	}
	b := wBucket{filename: filepath.Join(o.dir, bucket+".kv"), created: time.Now()}
	var err error
	if b.db, err = kv.Create(b.filename, nil); err != nil {
		return err
	}
	o.buckets[bucket] = b
	return nil
}

// CheckBucket returns whether the owner has a bucket named as given
func (m master) CheckBucket(owner s3intf.Owner, bucket string) bool {
	m.Lock()
	defer m.Unlock()
	if o, ok := m.owners[owner.ID()]; ok {
		o.Lock()
		_, ok = o.buckets[bucket]
		o.Unlock()
		return ok
	}
	return false
}

// DelBucket deletes a bucket
func (m master) DelBucket(owner s3intf.Owner, bucket string) error {
	m.Lock()
	defer m.Unlock()
	o, ok := m.owners[owner.ID()]
	if !ok {
		return fmt.Errorf("unknown owner %s", owner.ID())
	}
	o.Lock()
	defer o.Unlock()
	b, ok := o.buckets[bucket]
	if !ok {
		return fmt.Errorf("bucket %s not exists!", bucket)
	}
	if k, v, err := b.db.First(); err != nil {
		return err
	} else if k != nil || v != nil {
		return errors.New("cannot delete non-empty bucket")
	}
	b.db.Close()
	b.db = nil
	delete(o.buckets, bucket)
	return os.Remove(b.filename)
}

// List lists a bucket, all objects Key starts with prefix, delimiter segments
// Key, thus the returned commonprefixes (think a generalized filepath
// structure, where / is the delimiter, a commonprefix is a subdir)
func (m master) List(owner s3intf.Owner, bucket, prefix, delimiter, marker string,
	limit, skip int) (
	objects []s3intf.Object, commonprefixes []string,
	truncated bool, err error) {

	m.Lock()
	o, ok := m.owners[owner.ID()]
	if !ok {
		m.Unlock()
		err = fmt.Errorf("unknown owner %s", owner.ID())
		return
	}
	o.Lock()
	b, ok := o.buckets[bucket]
	o.Unlock()
	m.Unlock()
	if !ok {
		err = fmt.Errorf("unknown bucket %s", bucket)
		return
	}

	b.db.Seek()
	dh, e := os.Open(filepath.Join(string(root), owner.ID(), bucket))
	if e != nil {
		err = e
		return
	}
	defer dh.Close()
	var (
		infos []os.FileInfo
		early bool
	)
	n := 0
	for n <= skip {
		infos, e = dh.Readdir(limit)
		if e != nil {
			if e != io.EOF {
				err = e
				return
			}
			early = true
			break
		}
		n += len(infos)
	}
	if early {
		truncated = false
	} else {
		truncated = len(infos) < limit
	}
	//The prefix and delimiter parameters limit the kind of results returned by a list operation.
	//Prefix limits results to only those keys that begin with the specified prefix,
	//and delimiter causes list to roll up all keys that share a common prefix
	//into a single summary list result.
	var (
		i              int
		ok             bool
		prefixes       map[string]bool
		key, base, dir string
		plen           = len(prefix)
	)
	if delimiter != "" {
		prefixes = make(map[string]bool, 4)
	} else {
		i = -1
	}
	objects = make([]s3intf.Object, 0, len(infos))
	for _, fi := range infos {
		if key, _, _, err = decodeFilename(fi.Name()); err != nil {
			return
		}
		if prefix == "" || strings.HasPrefix(key, prefix) {
			if delimiter != "" {
				base = key[plen:]
				i = strings.Index(base, delimiter)
			}
			if i < 0 {
				objects = append(objects, s3intf.Object{Key: key,
					LastModified: fi.ModTime(), Size: fi.Size(), Owner: owner})
			} else { // delimiter != "" && delimiter in key[len(prefix):]
				dir = base[:i]
				if _, ok = prefixes[dir]; !ok {
					prefixes[dir] = true
				}
			}
		}
	}

	if len(prefixes) > 0 {
		commonprefixes = make([]string, 0, len(prefixes))
		for dir = range prefixes {
			commonprefixes = append(commonprefixes, dir)
		}
	}
	return
}

// Put puts a file as a new object into the bucket
func (m master) Put(owner s3intf.Owner, bucket, object, filename, media string, body io.Reader) error {
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
		if names, err = dh.Readdirnames(1000); err != nil && err != io.EOF {
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
func (m master) Get(owner s3intf.Owner, bucket, object string) (
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
func (m master) Del(owner s3intf.Owner, bucket, object string) error {
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
