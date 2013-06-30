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

package s3intf

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
)

var b64 = base64.StdEncoding
var debug = false

// GetOwner returns the Owner identified by the AccessKeyId in the request - if the authentication succeeds
// See http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader
func GetOwner(b Backer, r *http.Request, serviceHost string) (owner Owner, err error) {
	var access, signature string

	params := r.URL.Query()
	if _, ok := params["Expires"]; ok {
		// Query string request authentication alternative.
		//expires = true
		//date = v[0]
		access = params.Get("AWSAccessKeyId")
		signature = params.Get("Signature")
	}
	if access == "" || signature == "" {
		auth := r.Header.Get("Authorization")
		if auth != "" {
			// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;
			if strings.HasPrefix(auth, "AWS ") {
				auth = auth[4:]
			}
			i := strings.Index(auth, ":")
			if i < 0 {
				err = errors.New("no secret key?")
				return
			}
			access, signature = auth[:i], auth[i+1:]
		}
		if access == "" || signature == "" {
			err = errors.New("No authorization header")
			return
		}
	}

	// Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID, UTF-8-Encoding-Of( StringToSign ) ) );
	/*
		host := stripPort(r.Host)
		serviceHost = stripPort(serviceHost)
		if len(host) > len(serviceHost) {
			bucket = host[:len(host)-len(serviceHost)-1]
		}
	*/
	var o Owner
	if o, err = b.GetOwner(access); err != nil {
		return
	}
	h := o.GetHMAC(sha1.New)
	if _, err = h.Write(getBytesToSign(r, serviceHost)); err != nil {
		err = errors.New("hashing error: " + err.Error())
		return
	}
	actsign := b64.EncodeToString(h.Sum(nil))
	if actsign != signature {
		err = errors.New("signature mismatch (awaited " + signature +
			", got " + actsign + ")")
		return
	}
	return o, nil
}

func cr(w io.Writer) {
	w.Write([]byte{10}) //"\n"
}

// s3ParamsToSign is a map of parameter names which is needed to be in signature
// Copied from launchpad.net/goamz/s3/sign.go
var s3ParamsToSign = map[string]bool{
	"acl":                          true,
	"location":                     true,
	"logging":                      true,
	"notification":                 true,
	"partNumber":                   true,
	"policy":                       true,
	"requestPayment":               true,
	"torrent":                      true,
	"uploadId":                     true,
	"uploads":                      true,
	"versionId":                    true,
	"versioning":                   true,
	"versions":                     true,
	"response-content-type":        true,
	"response-content-language":    true,
	"response-expires":             true,
	"response-cache-control":       true,
	"response-content-disposition": true,
	"response-content-encoding":    true,
}

// getBytesToSign returns the StringToSign
// (see http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader)
// Most of it is copied from launchpad.net/goamz/s3/sign.go
func getBytesToSign(r *http.Request, serviceHost string) []byte {
	headers := r.Header
	params := r.URL.Query()
	if debug {
		log.Printf("headers: %s\nparams: %s", headers, params)
	}

	var md5, ctype, date, xamz string
	var xamzDate bool
	sarray := make([]string, 0, 4)
	for k, v := range headers {
		k = strings.ToLower(k)
		switch k {
		case "content-md5":
			md5 = v[0]
		case "content-type":
			ctype = v[0]
		case "date":
			if !xamzDate {
				date = v[0]
			}
		default:
			if strings.HasPrefix(k, "x-amz-") {
				vall := strings.Join(v, ",")
				sarray = append(sarray, k+":"+vall)
				if k == "x-amz-date" {
					xamzDate = true
					//When an x-amz-date header is present in a request,
					//the system will ignore any Date header when computing
					//the request signature. Therefore, if you include the
					//x-amz-date header, use the empty string for the Date
					//when constructing the StringToSign.
					date = ""
				}
			}
		}
	}
	if len(sarray) > 0 {
		sort.StringSlice(sarray).Sort()
		xamz = strings.Join(sarray, "\n") + "\n"
	}

	if v, ok := params["Expires"]; ok {
		// Query string request authentication alternative.
		date = v[0]
	}

	//return method + "\n" + md5 + "\n" + ctype + "\n" + date + "\n" + xamz + canonicalPath
	res := bytes.NewBuffer(make([]byte, 0, 64))
	res.WriteString(r.Method)
	for _, str := range []string{md5, ctype, date, xamz} {
		cr(res)
		res.WriteString(str)
	}
	// canonicalPath must start with "/" + Bucket
	if debug {
		log.Printf("host: %s => %s, serviceHost: %s => %s",
			r.Host, stripPort(r.Host),
			serviceHost, stripPort(serviceHost))
	}
	canonicalPath := ""
	host := stripPort(r.Host)
	if serviceHost == "" {
		canonicalPath = "/" + host
	} else {
		serviceHost = stripPort(serviceHost)
		if len(host) > len(serviceHost) { // bucket name is from host name
			canonicalPath = "/" + host[:len(host)-len(serviceHost)-1]
		}
	}
	//Append the path part of the un-decoded HTTP Request-URI,
	//up-to but not including the query string.
	uri := r.RequestURI
	i := strings.Index(uri, "://")
	if i < 0 {
		uri = uri[strings.Index(uri, "/"):]
	} else {
		uri = uri[i+3+strings.Index(uri[i+3:], "/"):]
	}
	if i = strings.Index(uri, "?"); i >= 0 {
		uri = uri[:i]
	}
	if debug {
		log.Printf("uri=%s => i=%d (%s)", r.RequestURI, i, uri)
	}
	canonicalPath += uri

	sarray = sarray[0:0]
	for k, v := range params {
		if s3ParamsToSign[k] {
			for _, vi := range v {
				if vi == "" {
					sarray = append(sarray, k)
				} else {
					// "When signing you do not encode these values."
					sarray = append(sarray, k+"="+vi)
				}
			}
		}
	}
	if len(sarray) > 0 {
		sort.StringSlice(sarray).Sort()
		canonicalPath += "?" + strings.Join(sarray, "&")
	}

	res.WriteString(canonicalPath)
	return res.Bytes()
}

func stripPort(text string) string {
	if text != "" {
		if i := strings.Index(text, ":"); i >= 0 {
			return text[:i]
		}
	}
	return text
}
