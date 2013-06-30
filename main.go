package main

import (
	"github.com/tgulacsi/s3weed/s3srv"
	"log"
	"net/http"
)

func main() {
	s3srv.Debug = true
    weed := s3weed.NewS3Backer(":9333")
    srvc := s3srv.NewService("localhost:8080", weed))
	log.Fatal(http.ListenAndServe(":8080", srvc)
}
