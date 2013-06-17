package s3weed

import (
	"bufio"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/textproto"
	"strconv"
)

type service string

func (host service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.RequestURI == "*" {
		writeBadRequest(w, "bad URI")
		return
	}
	if string(host) == r.Host { //Service level
		if r.Method != "GET" {
			writeBadRequest(w, "only GET allowed at service level")
			return
		}
		serviceGet(w, r)
		return
	}

	bucketHandler(r.Host[:len(r.Host)-len(string(host))-1]).ServeHTTP(w, r)
}

type bucketHandler string

func (bucket bucketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "" || r.Method == "POST" {
		objectHandler{string(bucket), r.URL.Path}.ServeHTTP(w, r)
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
	bucket, object string
}

func (obj objectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

//This implementation of the GET operation returns a list of all buckets owned by the authenticated sender of the request.
func serviceGet(w http.ResponseWriter, r *http.Request) {
	owner, buckets, err := backing.ListBuckets()
	if err != nil {
		writeISE(w, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	bw := bufio.NewWriter(w)
	bw.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
  <Owner><ID>` + owner.Id + "</ID><DisplayName>" + owner.Name + "</DisplayName></Owner><Buckets>")
	for _, bucket := range buckets {
		bw.WriteString("<Bucket><Name>" + bucket + "</Name></Bucket>")
		//<CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
	}
	bw.WriteString("</Buckets></ListAllMyBucketsResult>")
	bw.Flush()
}

//This implementation of the DELETE operation deletes the bucket named in the URI.
//All objects (including all object versions and Delete Markers) in the bucket
//must be deleted before the bucket itself can be deleted.
func (bucket bucketHandler) del(w http.ResponseWriter, r *http.Request) {
	if err := backing.DeleteBucket(string(bucket)); err != nil {
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

	objects, commonprefixes, truncated, err := backing.ListObjects(
		string(bucket), prefix, delimiter, marker, limit)
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
		string(bucket) + "</Name><Prefix>" + prefix + "</Prefix><Marker>" + marker +
		"</Marker><MaxKeys>" + strconv.Itoa(limit) + "</MaxKeys><IsTruncated>" +
		isTruncated + "</IsTruncate>")
	for _, object := range objects {
		bw.WriteString("<Contents><Key>" + object.Key + "</Key><Size>" +
			strconv.FormatInt(object.Size, 10) + "</Size><Owner><ID>" + object.Owner.Id +
			"</ID><DisplayName>" + object.Owner.Name +
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
}

//This implementation of the PUT operation creates a new bucket.
//Anonymous requests are never allowed to create buckets.
//By creating the bucket, you become the bucket owner.
//
//Not every string is an acceptable bucket name. For information on bucket naming restrictions, see Working with Amazon S3 Buckets.
//DNS name constraints -> max length is 63
func (bucket bucketHandler) put(w http.ResponseWriter, r *http.Request) {
}

func (obj objectHandler) del(w http.ResponseWriter, r *http.Request) {
	if err := backing.DeleteObject(obj.bucket, obj.object); err != nil {
		writeISE(w, fmt.Sprintf("error deleting %s/%s: %s", obj.bucket, obj.object, err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (obj objectHandler) get(w http.ResponseWriter, r *http.Request) {
	fn, media, body, err := backing.GetObject(obj.bucket, obj.object)
	if err != nil {
		if err == NotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeISE(w, fmt.Sprintf("error geting %s/%s: %s", obj.bucket, obj.object, err))
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
	if r.Body != nil {
		defer r.Body.Close()
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
			if _, params, err := mime.ParseMediaType(disp); err != nil {
				writeBadRequest(w, "cannot parse Content-Disposition "+disp+" "+err.Error())
				return
			} else {
				fn = params["filename"]
			}
		}
		body = r.Body
	}
	// Content-MD5 ?

	if err := backing.PutObject(obj.bucket, obj.object, fn, media, body); err != nil {
		if err == NotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeISE(w, fmt.Sprintf("error while storing %s in %s/%s: %s", fn, obj.bucket, obj.object, err))
		return
	}
	w.WriteHeader(http.StatusOK)
}
