package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/tgulacsi/s3weed/s3impl/dirS3"
	"github.com/tgulacsi/s3weed/s3impl/weedS3"
	"github.com/tgulacsi/s3weed/s3impl/weedS3/weedutils"
	"github.com/tgulacsi/s3weed/s3intf"
	"github.com/tgulacsi/s3weed/s3srv"

	"github.com/cznic/kv"
)

var (
	dir      = flag.String("dir", "", "use dirS3 with the given dir as base (i.e. -dir=/tmp)")
	weed     = flag.String("weed", "", "use weedS3 with the given master url (i.e. -weed=localhost:9333)")
	weedDb   = flag.String("db", "", "weedS3's db dir")
	hostPort = flag.String("http", ":8080", "host:port to listen on")
)

func main() {
	flag.Parse()
	cmd := "server"
	if flag.NArg() > 0 {
		cmd = flag.Arg(0)
	}
	switch cmd {
	case "dump":
		if *weedDb == "" {
			log.Fatalf("-db is needed to know what to dump!")
		}
		if err := dumpAll(*weedDb); err != nil {
			log.Fatalf("error dumping %s: %s", *weedDb, err)
		}

	default: //server
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
}

func dumpAll(dbdir string) error {
	dirs := make(chan string)
	go weedutils.ReadDirNames(dbdir,
		func(fi os.FileInfo) bool {
			return fi.IsDir()
		}, dirs)

	var (
		dn  string
		dbs chan *kv.DB
		err error
	)
	bw := bufio.NewWriter(os.Stdout)
	defer bw.Flush()
	bw.WriteString("[")

	for dn = range dirs {
		bw.WriteString(`{"owner": "` + filepath.Base(dn) + `", `)
		dbs = make(chan *kv.DB)
		go dumpBuckets(bw, dbs)
		if err = weedutils.OpenAllDb(dn, ".kv", dbs); err != nil {
			log.Printf("error opening db: %s", err)
		}
		bw.Flush()
	}
	os.Stdout.Close()
	return nil
}

func dumpBuckets(w io.Writer, dbs <-chan *kv.DB) {
	io.WriteString(w, `"buckets": [`)
	for db := range dbs {
		dumpBucket(w, db)
	}
	io.WriteString(w, "],\n")
}

func dumpBucket(w io.Writer, db *kv.DB) {
	io.WriteString(w, `{"name": "`+db.Name()+`", "records": [`)
	enc := json.NewEncoder(w)
	enum, err := db.SeekFirst()
	if err != nil {
		if err != io.EOF {
			log.Printf("error getting first: %s", err)
		}
	} else {
		var vi *weedutils.ValInfo
		for {
			k, v, err := enum.Next()
			if err != nil {
				if err != io.EOF {
					log.Printf("error getting next: %s", err)
				}
				break
			}
			if err = vi.Decode(v); err != nil {
				log.Printf("error decoding %s: %s", v, err)
				continue
			}
			fmt.Fprintf(w, `{"object": %q, "value": `, k)
			if err = enc.Encode(vi); err != nil {
				log.Printf("error printing %v to json: %s", vi, err)
				continue
			}
			io.WriteString(w, "},\n")
		}
		io.WriteString(w, "]},\n")
	}
	io.WriteString(w, "]},\n")
}
