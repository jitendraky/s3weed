package main

import (
	"github.com/tgulacsi/s3weed/s3impl/dirS3"
	"github.com/tgulacsi/s3weed/s3impl/weedS3"
	"github.com/tgulacsi/s3weed/s3srv"
	"log"
	"net/http"
)

func main() {
	s3srv.Debug = true
	//impl := dirS3.NewDirS3("/tmp")
	impl := weedS3.NewWeedS3("localhost:9333")
	srvc := s3srv.NewService("localhost:8080", impl)
	log.Fatal(http.ListenAndServe(":8080", srvc))
}
