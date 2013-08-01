package main

import (
	"github.com/tgulacsi/s3weed/s3impl/dirS3"
	"github.com/tgulacsi/s3weed/s3impl/weedS3"
	"github.com/tgulacsi/s3weed/s3intf"
	"github.com/tgulacsi/s3weed/s3srv"

	"flag"
	"log"
	"net/http"
)

var (
	dir      = flag.String("dir", "", "use dirS3 with the given dir as base (i.e. -dir=/tmp)")
	weed     = flag.String("weed", "", "use weedS3 with the given master url (i.e. -weed=localhost:9333)")
	weedDb   = flag.String("db", "", "weedS3's db dir")
	hostPort = flag.String("http", ":8080", "host:port to listen on")
)

func main() {
	flag.Parse()

	s3srv.Debug = true
    s3intf.Debug = true
	var (
		impl s3intf.Storage
		err  error
	)
	if *dir != "" {
		impl = dirS3.NewDirS3(*dir)
	} else if *weed != "" && *weedDb != "" {
		if impl, err = weedS3.NewWeedS3(*weed, *weedDb); err != nil {
			log.Fatalf("cannot create WeedS3(%s, %s): %s", *weed, *weedDb, err)
		}
	} else {
		log.Fatalf("dir OR weed AND db is required!")
	}
	srvc := s3srv.NewService(*hostPort, impl)
	log.Fatal(http.ListenAndServe(*hostPort, srvc))
}
