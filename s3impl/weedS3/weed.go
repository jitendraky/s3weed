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

	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/gob"
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

var kvOptions = new(kv.Options)

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

func (o wOwner) ID() string {
	return filepath.Base(o.dir)
}

// Name returns then name of this owner
func (o wOwner) Name() string {
	return filepath.Base(o.dir)
}

// GetHMAC returns a HMAC initialized with the secret key
func (o wOwner) GetHMAC(h func() hash.Hash) hash.Hash {
	return hmac.New(h, nil)
}

// Check checks the validity of the authorization
func (o wOwner) CalcHash(bytesToSign []byte) []byte {
	return s3intf.CalcHash(hmac.New(sha1.New, nil), bytesToSign)
}

type master struct {
	wm      weedMaster // master weed node's URL
	baseDir string
	owners  map[string]wOwner
	sync.Mutex
}

// GetOwner returns the Owner for the accessKey - or an error
func (m master) GetOwner(accessKey string) (s3intf.Owner, error) {
	m.Lock()
	defer m.Unlock()
	if o, ok := m.owners[accessKey]; ok {
		return o, nil
	}
	return nil, errors.New("owner " + accessKey + " not found")
}

// NewWeedS3 stores everything in the given master Weed-FS node
// buckets are stored
func NewWeedS3(masterURL, dbdir string) (s3intf.Storage, error) {
	m := master{wm: newWeedMaster(masterURL), baseDir: dbdir, owners: nil}
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
	b.db, err = kv.Open(filename, kvOptions)
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
	} else if o.buckets == nil {
		o.buckets = make(map[string]wBucket, 1)
	}
	o.Lock()
	defer o.Unlock()
	_, ok = o.buckets[bucket]
	if ok {
		return nil //AlreadyExists ?
	}
	b := wBucket{filename: filepath.Join(o.dir, bucket+".kv"), created: time.Now()}
	var err error
	if b.db, err = kv.Create(b.filename, kvOptions); err != nil {
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

	err = nil
	enum, e := b.db.SeekFirst()
	if e != nil {
        if e == io.EOF { //empty
            return
        }
		err = fmt.Errorf("error getting first: %s", e)
		return
	}
	var (
		key, val []byte
		created  time.Time
		size     int64
	)
	objects = make([]s3intf.Object, 0, 64)
	f := s3intf.NewListFilter(prefix, delimiter, marker, limit, skip)
	for {
		if key, val, e = enum.Next(); e != nil {
			if e == io.EOF {
				break
			}
			err = fmt.Errorf("error seeking next: %s", e)
			return
		}
		if ok, e = f.Check(string(key)); e != nil {
			if e == io.EOF {
				commonprefixes, truncated = f.Result()
				return
			}
			err = fmt.Errorf("error checking %s: %s", key, e)
			return
		} else if ok {
			_, _, _, created, size = decodeVal(val)
			objects = append(objects,
				s3intf.Object{Key: string(key), Owner: owner,
					LastModified: created, Size: size})
		}
	}
	commonprefixes, truncated = f.Result()
	return
}

// Put puts a file as a new object into the bucket
func (m master) Put(owner s3intf.Owner, bucket, object, filename, media string, body io.Reader, size int64) error {
	m.Lock()
	o, ok := m.owners[owner.ID()]
	m.Unlock()
	if !ok {
		return errors.New("cannot find owner " + owner.ID())
	}
	o.Lock()
	b, ok := o.buckets[bucket]
	o.Unlock()
	if !ok {
		return errors.New("cannot find bucket " + bucket)
	}

	err := b.db.BeginTransaction()
	if err != nil {
		return fmt.Errorf("cannot start transaction: %s", err)
	}
	//upload
	resp, err := m.wm.assignFid()
	if err != nil {
		return fmt.Errorf("error getting fid: %s", err)
	}
	err = b.db.Set([]byte(object), encodeVal(filename, media, resp.Fid, time.Now(), size))
	if err != nil {
		return fmt.Errorf("error storing key in db: %s", err)
	}
	if _, err = m.wm.upload(resp, filename, media, body); err != nil {
		b.db.Rollback()
		return fmt.Errorf("error uploading to %s: %s", resp.Fid, err)
	}
	return b.db.Commit()
}

type valInfo struct {
	filename, media, fid string
	created              time.Time
	size                 int64
}

func encodeVal(filename, contentType, fid string, created time.Time, size int64) []byte {
	buf := bytes.NewBuffer(make([]byte, len(filename)+len(contentType)+len(fid)+8+8+8))
	enc := gob.NewEncoder(buf)
	enc.Encode(filename)
	enc.Encode(contentType)
	enc.Encode(fid)
	enc.Encode(created)
	enc.Encode(size)
	return buf.Bytes()
}
func decodeVal(val []byte) (filename, contentType, fid string, created time.Time, size int64) {
	dec := gob.NewDecoder(bytes.NewReader(val))
	dec.Decode(&filename)
	dec.Decode(&contentType)
	dec.Decode(&fid)
	dec.Decode(&created)
	dec.Decode(&size)
	return
}

// Get retrieves an object from the bucket
func (m master) Get(owner s3intf.Owner, bucket, object string) (
	filename, media string, body io.ReadCloser, err error) {

	m.Lock()
	o, ok := m.owners[owner.ID()]
	m.Unlock()
	if !ok {
		err = errors.New("cannot find owner " + owner.ID())
		return
	}
	o.Lock()
	b, ok := o.buckets[bucket]
	o.Unlock()
	if !ok {
		err = errors.New("cannot find bucket " + bucket)
		return
	}

	val, e := b.db.Get(nil, []byte(object))
	if e != nil {
		err = fmt.Errorf("cannot get %s object: %s", object, e)
		return
	}
	var fid string
	filename, media, fid, _, _ = decodeVal(val)

	body, err = m.wm.download(fid)
	return
}

// Del deletes the object from the bucket
func (m master) Del(owner s3intf.Owner, bucket, object string) error {
	m.Lock()
	o, ok := m.owners[owner.ID()]
	m.Unlock()
	if !ok {
		return errors.New("cannot find owner " + owner.ID())
	}
	o.Lock()
	b, ok := o.buckets[bucket]
	o.Unlock()
	if !ok {
		return errors.New("cannot find bucket " + bucket)
	}

	b.db.BeginTransaction()
	val, err := b.db.Get(nil, []byte(object))
	if err != nil {
		b.db.Rollback()
		return fmt.Errorf("cannot get %s object: %s", object, err)
	}
	_, _, fid, _, _ := decodeVal(val)
	if err = m.wm.delete(fid); err != nil {
		b.db.Rollback()
		return err
	}
	return b.db.Commit()
}
