package main

import (
	"github.com/tgulacsi/s3weed/s3impl/dirS3"
	"github.com/tgulacsi/s3weed/s3impl/weedS3"
	"github.com/tgulacsi/s3weed/s3intf"
	"github.com/tgulacsi/s3weed/s3srv"

	"github.com/cznic/kv"

	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
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

type record struct {
	Filename, ContentType, Fid string
	Created                    time.Time
	Size                       int64
}

func dumpAll(dbdir string) error {
	dh, err := os.Open(dbdir)
	if err != nil {
		return fmt.Errorf("error opening %s: %s", dbdir, err)
	}
	defer dh.Close()
	var (
		kvOptions = new(kv.Options)
		fn, bn    string
		fi        os.FileInfo
		db        *kv.DB
		rec       record
	)
	bw := bufio.NewWriter(os.Stdout)
	defer bw.Flush()
	bw.WriteString("[")
	enc := json.NewEncoder(bw)
	for {
		fis, err := dh.Readdir(1000)
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("error reading dir %s: %s", dh.Name(), err)
			}
			break
		}
		for _, fi = range fis {
			if !fi.IsDir() {
				continue
			}
			fn = filepath.Join(dbdir, fi.Name())
			sdh, err := os.Open(fn)
			if err != nil {
				log.Printf("error opening %s: %s", fi.Name(), err)
				continue
			}
			for {
				sfis, err := sdh.Readdir(1000)
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Printf("error reading dir %s: %s", sdh.Name(), err)
				}
				bw.WriteString(`{"owner": "` + sdh.Name() + `", "buckets": [`)
				for _, fi = range sfis {
					if !(fi.Mode().IsRegular() && strings.HasSuffix(fi.Name(), ".kv")) {
						continue
					}
					fn = filepath.Join(sdh.Name(), fi.Name())
					if db, err = kv.Open(fn, kvOptions); err != nil {
						log.Printf("error opening buckets db %s: %s", fn, err)
						continue
					}
					bn = fi.Name()
					bn = bn[:len(bn)-3]
					enum, err := db.SeekFirst()
					if err != nil {
						if err != io.EOF {
							log.Printf("error getting first: %s", err)
						} else {
							log.Printf("%s is empty!", db)
							bw.WriteString(`{"name": "` + bn + `", "records": []},` + "\n")
						}
					} else {
						bw.WriteString(`{"name": "` + bn + `", "records": [`+"\n")
						for {
							k, v, err := enum.Next()
							if err != nil {
								if err != io.EOF {
									log.Printf("error getting next: %s", err)
								}
								break
							}
							rec.Filename, rec.ContentType, rec.Fid, rec.Created, rec.Size = decodeVal(v)
							bw.WriteString(fmt.Sprintf(`{"object": %q, "value": `, k))
							if err = enc.Encode(rec); err != nil {
								log.Printf("error printing %v to json: %s", rec, err)
								continue
							}
							bw.WriteString("},\n")
						}
						bw.WriteString("]},\n")
					}
					db.Close()
				}
				bw.WriteString("]},\n")
				bw.Flush()
			}
			sdh.Close()
		}
	}
	bw.WriteString("]\n")
	return nil
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
