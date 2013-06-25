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
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
)

// http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader
func getOwner(r *http.Request, host string) (owner Owner, err error) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		err = errors.New("No authorization header")
		return
	}
	// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;
	if strings.HasPrefix(auth, "AWS ") {
		auth = auth[4:]
	}
	i := strings.Index(auth, ":")
	if i < 0 {
		err = errors.New("no secret key?")
		return
	}
	access, signature := auth[:i], auth[i+1:]

	// Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID, UTF-8-Encoding-Of( StringToSign ) ) );
	bucket := ""
	if len(r.Host) > len(host) {
		bucket = r.Host[:len(r.Host)-len(host)-1]
	}
	var o Owner
	if o, err = backing.GetOwner(access); err != nil {
		return
	}
	if base64.StdEncoding.EncodeToString(o.Sign(getStringToSign(r, bucket))) != signature {
		err = errors.New("signature mismatch")
		return
	}
	return o, nil
}

func cr(w io.Writer) {
	w.Write([]byte{10}) //CR
}

func getStringToSign(r *http.Request, serviceHost string) []byte {
	// StringToSign = HTTP-Verb + "\n" +
	//  Content-MD5 + "\n" +
	//  Content-Type + "\n" +
	//  Date + "\n" +
	//  CanonicalizedAmzHeaders +
	//  CanonicalizedResource;
	res := bytes.NewBuffer(make([]byte, 64))
	res.WriteString(r.Method)
	cr(res)
	for _, k := range [...]string{"Content-MD5", "Content-Type"} {
		res.WriteString(r.Header.Get(k))
		cr(res)
	}
	qry := r.URL.Query()
	if exp, ok := qry["Expires"]; ok {
		res.WriteString(exp[0])
	} else {
		res.WriteString(r.Header.Get("Date"))
	}
	cr(res)
	appendCanonicalizedResource(res, r, serviceHost)
	appendCanonicalizedAmzHeaders(res, r)

	return res.Bytes()
}

func appendCanonicalizedResource(w io.Writer, r *http.Request, serviceHost string) {
	//For a virtual hosted-style request "https://johnsmith.s3.amazonaws.com/photos/puppy.jpg", the CanonicalizedResource is "/johnsmith".
	//For the path-style request, "https://s3.amazonaws.com/johnsmith/photos/puppy.jpg", the CanonicalizedResource is "".
	if serviceHost != "" && len(serviceHost) < len(r.Host) {
		io.WriteString(w, "/")
		io.WriteString(w, r.Host[:len(r.Host)-len(serviceHost)-1])
	}

	//Append the path part of the un-decoded HTTP Request-URI, up-to but not including the query string.
	//For a virtual hosted-style request "https://johnsmith.s3.amazonaws.com/photos/puppy.jpg", the CanonicalizedResource is "/johnsmith/photos/puppy.jpg".
	//For a path-style request, "https://s3.amazonaws.com/johnsmith/photos/puppy.jpg", the CanonicalizedResource is "/johnsmith/photos/puppy.jpg". At this point, the CanonicalizedResource is the same for both the virtual hosted-style and path-style request.
	io.WriteString(w, "/")
	io.WriteString(w, r.URL.Path)

	//If the request addresses a sub-resource, like ?versioning, ?location, ?acl, ?torrent, ?lifecycle, or ?versionid append the sub-resource, its value if it has one, and the question mark. Note that in case of multiple sub-resources, sub-resources must be lexicographically sorted by sub-resource name and separated by '&'. e.g. ?acl&versionId=value.
	//The list of sub-resources that must be included when constructing the CanonicalizedResource Element are: acl, lifecycle, location, logging, notification, partNumber, policy, requestPayment, torrent, uploadId, uploads, versionId, versioning, versions and website.
	i := 0
	var v string
	for _, k := range [...]string{ // sorted lexicographically
		"acl",
		"delete",
		"lifecycle",
		"location",
		"logging",
		"notification",
		"partNumber",
		"policy",
		"requestPayment",
		"response-cache-control",
		"response-content-disposition",
		"response-content-encoding",
		"response-content-language",
		"response-content-type",
		"response-expires",
		"torrent",
		"uploadId",
		"uploads",
		"versionId",
		"versioning",
		"versions",
		"website"} {

		if v = r.Header.Get(k); v == "" {
			continue
		}
		if i == 0 {
			io.WriteString(w, "?")
		} else {
			io.WriteString(w, "&")
		}
		io.WriteString(w, k)
		io.WriteString(w, "=")
		io.WriteString(w, v)
	}
	//If the request specifies query string parameters overriding the response header values (see Get Object), append the query string parameters, and its values. When signing you do not encode these values. However, when making the request, you must encode these parameter values. The query string parameters in a GET request include response-content-type, response-content-language, response-expires, response-cache-control, response-content-disposition, response-content-encoding.
	//The delete query string parameter must be including when creating the CanonicalizedResource for a Multi-Object Delete request.
}

func appendCanonicalizedAmzHeaders(w io.Writer, r *http.Request) {
	//1. Convert each HTTP header name to lower-case. For example, 'X-Amz-Date' becomes 'x-amz-date'.
	//2. Sort the collection of headers lexicographically by header name.
	keys := make([]string, len(r.Header)/2)
	qry := r.URL.Query()
	for k, _ := range qry {
		if strings.HasPrefix(k, "X-Amz-") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	//3. Combine header fields with the same name into one "header-name:comma-separated-value-list" pair as prescribed by RFC 2616, section 4.2, without any white-space between values. For example, the two metadata headers 'x-amz-meta-username: fred' and 'x-amz-meta-username: barney' would be combined into the single header 'x-amz-meta-username: fred,barney'.
	var i int
	var v string
	for _, k := range keys {
		//4. "Unfold" long headers that span multiple lines (as allowed by RFC 2616, section 4.2) by replacing the folding white-space (including new-line) by a single space.
		//5. Trim any white-space around the colon in the header. For example, the header 'x-amz-meta-username: fred,barney' would become 'x-amz-meta-username:fred,barney'
		io.WriteString(w, strings.ToLower(k))
		io.WriteString(w, ":")
		for i, v = range qry[k] {
			if i != 0 {
				io.WriteString(w, ",")
			}
			io.WriteString(w, v)
		}
		//6. Finally, append a new-line (U+000A) to each canonicalized header in the resulting list. Construct the CanonicalizedResource element by concatenating all headers in this list into a single string.
		cr(w)
	}
}
