package main

import (
    "context"
    "fmt"
    "net/http"

    //~ "github.com/tus/tusd/v2/pkg/filestore"
    "github.com/tus/tusd/v2/pkg/s3store"
    "github.com/tus/tusd/v2/pkg/memorylocker"
    tusd "github.com/tus/tusd/v2/pkg/handler"
    
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    
    nestore "github.com/gpo-geo/nestor/store"
)

var Flags struct {
    S3Bucket                         string
    S3ObjectPrefix                   string
    S3Endpoint                       string
    S3PartSize                       int64
    S3MaxBufferedParts               int64
    S3DisableContentHashes           bool
    S3DisableSSL                     bool
    S3ConcurrentPartUploads          int
    S3TransferAcceleration           bool
}

func setup_default_flags() {
    Flags.S3Bucket = "gpo"
    Flags.S3ObjectPrefix = ""
    Flags.S3Endpoint = "http://localhost:9000"
    Flags.S3PartSize = 50*1024*1024
    Flags.S3MaxBufferedParts = 20
    Flags.S3DisableContentHashes = true
    Flags.S3DisableSSL = true
    Flags.S3ConcurrentPartUploads = 10
    Flags.S3TransferAcceleration = false
}

func main() {

    setup_default_flags()
    
    // Create a S3 storage based on Minio
    s3Config, err := config.LoadDefaultConfig(context.Background())
    if err != nil {
        panic(fmt.Errorf("Unable to load S3 configuration: %s", err))
    }

    s3Client := s3.NewFromConfig(s3Config, func(o *s3.Options) {
        o.UseAccelerate = Flags.S3TransferAcceleration
    
        // Disable HTTPS and only use HTTP (helpful for debugging requests).
        o.EndpointOptions.DisableHTTPS = Flags.S3DisableSSL
    
        if Flags.S3Endpoint != "" {
            o.BaseEndpoint = &Flags.S3Endpoint
            o.UsePathStyle = true
        }
    })
    
    cryptClient, _ := nestore.NewEncryptedS3(s3Client, "champignon")
    
    store := s3store.New(Flags.S3Bucket, cryptClient)
    store.ObjectPrefix = Flags.S3ObjectPrefix
    store.PreferredPartSize = Flags.S3PartSize
    store.MaxBufferedParts = Flags.S3MaxBufferedParts
    store.DisableContentHashes = Flags.S3DisableContentHashes
    store.SetConcurrentPartUploads(Flags.S3ConcurrentPartUploads)
    
    locker := memorylocker.New()

    // A storage backend for tusd may consist of multiple different parts which
    // handle upload creation, locking, termination and so on. The composer is a
    // place where all those separated pieces are joined together. In this example
    // we only use the file store but you may plug in multiple.
    composer := tusd.NewStoreComposer()
    store.UseIn(composer)
    locker.UseIn(composer)

    // Create a new HTTP handler for the tusd server by providing a configuration.
    // The StoreComposer property must be set to allow the handler to function.
    handler, err := tusd.NewHandler(tusd.Config{
        BasePath:              "/files/",
        StoreComposer:         composer,
        NotifyCompleteUploads: true,
    })
    if err != nil {
        panic(fmt.Errorf("Unable to create handler: %s", err))
    }

    // Start another goroutine for receiving events from the handler whenever
    // an upload is completed. The event will contains details about the upload
    // itself and the relevant HTTP request.
    go func() {
        for {
            event := <-handler.CompleteUploads
            fmt.Printf("Upload %s finished\n", event.Upload.ID)
        }
    }()

    // Right now, nothing has happened since we need to start the HTTP server on
    // our own. In the end, tusd will start listening on and accept request at
    // http://localhost:8080/files
    http.Handle("/files/", http.StripPrefix("/files/", handler))
    err = http.ListenAndServe(":8080", nil)
    if err != nil {
        panic(fmt.Errorf("Unable to listen: %s", err))
    }
}
