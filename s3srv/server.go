/*
Package s3srv provides an S3 compatible server using s3intf.Storage

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
package s3srv

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/tgulacsi/s3weed/s3intf"
	"io"
	"log"
	"mime"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

// Debug prints
var Debug bool

// NotFound prints Not Found
var NotFound = errors.New("Not Found")

type service struct {
	fqdn string
	s3intf.Storage
}

// NewService returns a new service
func NewService(fqdn string, provider s3intf.Storage) *service {
	return &service{fqdn: fqdn, Storage: provider}
}

func (s *service) Host() string {
	return s.fqdn
}

func (host *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if Debug {
		log.Printf("%s.ServeHTTP %s %s", host.fqdn, r.Method, r.RequestURI)
	}
	if r.RequestURI == "*" || r.Host == "" || r.URL == nil || r.URL.Path == "" {
		writeBadRequest(w, "bad URI")
		return
	}
	if host.fqdn == r.Host { //Service level
		log.Printf("service level request, path: %s", r.URL.Path)
		if r.URL.Path != "/" {
			segments := strings.SplitN(r.URL.Path[1:], "/", 2)
			bucketHandler{Name: segments[0], Service: host}.ServeHTTP(w, r)
			return
		}
		if r.Method != "GET" {
			writeBadRequest(w, "only GET allowed at service level")
			return
		}
		host.serviceGet(w, r)
		return
	}

	bucketHandler{Name: r.Host[:len(r.Host)-len(host.fqdn)-1],
		Service: host, VirtualHost: true}.ServeHTTP(w, r)
}

type bucketHandler struct {
	Name        string
	Service     *service
	VirtualHost bool
}

func (bucket bucketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if Debug {
		log.Printf("bucket %s", bucket)
	}
	path := r.URL.Path
	if !bucket.VirtualHost {
		path = path[len(bucket.Name)+1:]
	}
	if path != "/" || r.Method == "POST" {
		objectHandler{Bucket: bucket, object: path}.ServeHTTP(w, r)
		return
	}
	switch r.Method {
	case "DELETE":
		bucket.del(w, r)
	case "GET":
		bucket.list(w, r)
	case "HEAD":
		bucket.check(w, r)
	case "PUT":
		bucket.put(w, r)
	default:
		writeBadRequest(w, "only DELETE, GET, HEAD and PUT allowed at bucket level")
	}
}

type objectHandler struct {
	Bucket bucketHandler
	object string
}

func (obj objectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if Debug {
		log.Printf("object %s", obj)
	}
	switch r.Method {
	case "DELETE":
		obj.del(w, r)
	case "GET":
		obj.get(w, r)
	case "PUT", "POST":
		obj.put(w, r)
	default:
		writeBadRequest(w, "only DELETE, GET, PUT and POST allowed at object level")
	}
}

func writeBadRequest(w http.ResponseWriter, message string) {
	log.Printf("bad request %s", message)
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusBadRequest)
	if message != "" {
		w.Write([]byte(message))
	}
}

func writeISE(w http.ResponseWriter, message string) {
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusInternalServerError)
	if message != "" {
		w.Write([]byte(message))
	}
}

// ValidBucketName returns whether name is a valid bucket name.
// Here are the rules, from:
// http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/BucketRestrictions.html
//
// Can contain lowercase letters, numbers, periods (.), underscores (_),
// and dashes (-). You can use uppercase letters for buckets only in the
// US Standard region.
//
// Must start with a number or letter
//
// Must be between 3 and 255 characters long
//
// There's one extra rule (Must not be formatted as an IP address (e.g., 192.168.5.4)
// but the real S3 server does not seem to check that rule, so we will not
// check it either.
//
func ValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 255 {
		return false
	}
	r := name[0]
	if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'z') {
		return false
	}
	for _, r := range name {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'z':
		case r == '_' || r == '-':
		case r == '.':
		default:
			return false
		}
	}
	return true
}

//This implementation of the GET operation returns a list of all buckets owned by the authenticated sender of the request.
func (s *service) serviceGet(w http.ResponseWriter, r *http.Request) {
	owner, err := s3intf.GetOwner(s, r, s.fqdn)
	log.Printf("%#v.serviceGet owner=%s err=%s", s, owner, err)
	if err != nil {
		writeISE(w, "error getting owner: "+err.Error())
		return
	} else if owner == nil {
		writeBadRequest(w, "no owner")
		return
	}
	buckets, err := s.ListBuckets(owner)
	if err != nil {
		writeISE(w, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	bw := bufio.NewWriter(w)
	bw.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
  <Owner><ID>` + owner.ID() + "</ID><DisplayName>" + owner.Name() + "</DisplayName></Owner><Buckets>")
	for _, bucket := range buckets {
		bw.WriteString("<Bucket><Name>" + bucket.Name + "</Name>")
		bw.WriteString("<CreationDate>" + bucket.Created.Format(time.RFC3339) +
			"</CreationDate></Bucket>")
	}
	bw.WriteString("</Buckets></ListAllMyBucketsResult>")
	bw.Flush()
}

//This implementation of the DELETE operation deletes the bucket named in the URI.
//All objects (including all object versions and Delete Markers) in the bucket
//must be deleted before the bucket itself can be deleted.
func (bucket bucketHandler) del(w http.ResponseWriter, r *http.Request) {
	owner, err := s3intf.GetOwner(bucket.Service, r, bucket.Service.fqdn)
	if err != nil {
		writeBadRequest(w, "error getting owner: "+err.Error())
		return
	}
	if err := bucket.Service.DelBucket(owner, bucket.Name); err != nil {
		if err == NotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeISE(w, err.Error())
		return
	}
}

//This implementation of the GET operation returns some or all (up to 1000)
//of the objects in a bucket.
//You can use the request parameters as selection criteria to return a subset
//of the objects in a bucket.
//
//To use this implementation of the operation, you must have READ access to the bucket.
func (bucket bucketHandler) list(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		writeBadRequest(w, "cannot parse form values")
		return
	}
	delimiter := r.Form.Get("delimiter")
	marker := r.Form.Get("marker")
	limit := 1000
	maxkeys := r.Form.Get("max-keys")
	if maxkeys != "" {
		if limit, err = strconv.Atoi(maxkeys); err != nil {
			writeBadRequest(w, "cannot parse max-keys value: "+err.Error())
			return
		}
	}
	prefix := r.Form.Get("prefix")

	owner, err := s3intf.GetOwner(bucket.Service, r, bucket.Service.fqdn)
	if err != nil {
		writeBadRequest(w, "error getting owner: "+err.Error())
		return
	}
	objects, commonprefixes, truncated, err := bucket.Service.List(owner,
		bucket.Name, prefix, delimiter, marker, limit)
	if err != nil {
		if err == NotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeISE(w, err.Error())
		return
	}
	isTruncated := "false"
	if truncated {
		isTruncated = "true"
	}

	w.Header().Set("Content-Type", "text/xml")
	bw := bufio.NewWriter(w)
	bw.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>` +
		bucket.Name + "</Name><Prefix>" + prefix + "</Prefix><Marker>" + marker +
		"</Marker><MaxKeys>" + strconv.Itoa(limit) + "</MaxKeys><IsTruncated>" +
		isTruncated + "</IsTruncate>")
	for _, object := range objects {
		bw.WriteString("<Contents><Key>" + object.Key + "</Key><Size>" +
			strconv.FormatInt(object.Size, 10) + "</Size><Owner><ID>" + object.Owner.ID() +
			"</ID><DisplayName>" + object.Owner.Name() +
			"</DisplayName></Owner></Contents>")
	}
	for _, cp := range commonprefixes {
		bw.WriteString("<CommonPrefixes><Prefix>" + cp + "</Prefix></CommonPrefixes>")
	}
	bw.WriteString("</ListBucketResult>")
	bw.Flush()
}

//This operation is useful to determine if a bucket exists and you have permission to access it.
//The operation returns a 200 OK if the bucket exists and you have permission to access it.
//Otherwise, the operation might return responses such as 404 Not Found and 403 Forbidden.
func (bucket bucketHandler) check(w http.ResponseWriter, r *http.Request) {
	owner, err := s3intf.GetOwner(bucket.Service, r, bucket.Service.Host())
	if err != nil {
		writeBadRequest(w, "error getting owner: "+err.Error())
		return
	}
	if bucket.Service.CheckBucket(owner, bucket.Name) {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNotFound)
	return
}

//This implementation of the PUT operation creates a new bucket.
//Anonymous requests are never allowed to create buckets.
//By creating the bucket, you become the bucket owner.
//
//Not every string is an acceptable bucket name. For information on bucket naming restrictions, see Working with Amazon S3 Buckets.
//DNS name constraints -> max length is 63
func (bucket bucketHandler) put(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s.put", bucket)
	owner, err := s3intf.GetOwner(bucket.Service, r, bucket.Service.Host())
	if err != nil {
		writeBadRequest(w, "error getting owner: "+err.Error())
		return
	}
	log.Printf("creating bucket %s for %s", bucket.Name, owner)
	if err := bucket.Service.CreateBucket(owner, bucket.Name); err != nil {
		writeISE(w, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	return
}

func (obj objectHandler) del(w http.ResponseWriter, r *http.Request) {
	owner, err := s3intf.GetOwner(obj.Bucket.Service, r, obj.Bucket.Service.Host())
	if err != nil {
		writeBadRequest(w, "error getting owner: "+err.Error())
		return
	}
	if err := obj.Bucket.Service.Del(owner, obj.Bucket.Name, obj.object); err != nil {
		writeISE(w, fmt.Sprintf("error deleting %s/%s: %s", obj.Bucket, obj.object, err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (obj objectHandler) get(w http.ResponseWriter, r *http.Request) {
	owner, err := s3intf.GetOwner(obj.Bucket.Service, r, obj.Bucket.Service.Host())
	if err != nil {
		writeBadRequest(w, "error getting owner: "+err.Error())
		return
	}
	fn, media, body, err := obj.Bucket.Service.Get(owner, obj.Bucket.Name, obj.object)
	if err != nil {
		if err == NotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeISE(w, fmt.Sprintf("error geting %s/%s: %s", obj.Bucket, obj.object, err))
		return
	}
	if err = r.ParseForm(); err != nil {
		writeBadRequest(w, "cannot parse form values")
		return
	}
	w.Header().Set("Content-Type", media)
	w.Header().Set("Content-Disposition", "inline; filename=\""+fn+"\"")
	for k, v := range r.Form {
		k = textproto.CanonicalMIMEHeaderKey(k)
		switch k {
		case "Content-Type", "Content-Language", "Expires", "Cache-Control",
			"Content-Disposition", "Content-Encoding":
			(map[string][]string(w.Header()))[k] = v
		}
	}
	io.Copy(w, body)
}

func (obj objectHandler) put(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		writeBadRequest(w, "nil body")
		return
	}
	defer r.Body.Close()
	owner, err := s3intf.GetOwner(obj.Bucket.Service, r, obj.Bucket.Service.Host())
	if err != nil {
		writeBadRequest(w, "error getting owner: "+err.Error())
		return
	}
	var fn, media string
	var body io.Reader
	if r.Method == "POST" {
		mpf, mph, err := r.FormFile("file")
		if err != nil {
			return
		}
		fn = mph.Filename
		media = mph.Header.Get("Content-Type")
		body = mpf
	} else {
		media = r.Header.Get("Content-Type")
		if disp := r.Header.Get("Content-Disposition"); disp != "" {
			if _, params, err := mime.ParseMediaType(disp); err == nil {
				fn = params["filename"]
			} else {
				writeBadRequest(w, "cannot parse Content-Disposition "+disp+" "+err.Error())
				return
			}
		}
		body = r.Body
	}
	// Content-MD5 ?

	if err := obj.Bucket.Service.Put(owner, obj.Bucket.Name, obj.object,
		fn, media, body); err != nil {
		if err == NotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeISE(w, fmt.Sprintf("error while storing %s in %s/%s: %s", fn, obj.Bucket, obj.object, err))
		return
	}
	w.WriteHeader(http.StatusOK)
}
