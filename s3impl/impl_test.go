package main

import (
	"encoding/base64"
	"github.com/tgulacsi/s3weed/s3impl/dirS3"
	"github.com/tgulacsi/s3weed/s3intf"
	"github.com/tgulacsi/s3weed/s3srv"
	"net/http"
	"net/http/httptest"
	"testing"
)

var b64 = base64.StdEncoding

var backers = make([]s3intf.Storage, 0, 1)
var handlers = make([]http.Handler, 0, 1)
var serviceHost = "s3.test.org"

func Test01ListBuckets(t *testing.T) {
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("cannot create request: " + err.Error())
	}
	req.Host = serviceHost
	//req.URL.Host = req.Host
	var o s3intf.Owner
	for i, b := range backers {
		if o, err = b.GetOwner("test"); err != nil {
			t.Errorf("cannot get owner for test: %s", err)
			continue
		}
		t.Logf("===")
		s3intf.Debug = true
		bts := s3intf.GetBytesToSign(req, serviceHost)
		t.Logf("owner: %s bts=%q", o, bts)
		actsign := b64.EncodeToString(o.CalcHash(bts))
		req.Header.Set("Authorization", "AWS test:"+actsign)

		rw := httptest.NewRecorder()
		handlers[i].ServeHTTP(rw, req)
		if rw.Code != 200 {
			t.Errorf("bad response code: %d (%s)", rw.Code, rw.Body.Bytes())
		}
	}
}

func init() {
	s3srv.Debug = true
	s3intf.Debug = false
	backers = append(backers, dirS3.NewDirS3("/tmp"))

	for _, b := range backers {
		handlers = append(handlers, s3srv.NewService(serviceHost, b))
	}
}
