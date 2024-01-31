package cmd

import (
    "github.com/gpo-geo/nestor/filters"
    "fmt"
    "io/ioutil"
    "net/http"
    "log"
    "strconv"
)

const apiVer = 1
var apiPrefix string = "/api/v"+strconv.Itoa(apiVer)
var uploadEndpoint string = apiPrefix + "/upload/"
var downloadEndpoint string = apiPrefix + "/download/"

const MAX_UPLOAD_SIZE = 10 * 1024 * 1024 // 10MB

func Upload(w http.ResponseWriter, req *http.Request) {

    if req.Method != "POST" {
        http.Error(w, "Use POST method", http.StatusMethodNotAllowed)
        return
    }

    req.Body = http.MaxBytesReader(w, req.Body, MAX_UPLOAD_SIZE)
    body, err := ioutil.ReadAll(req.Body)
    if err != nil {
        log.Printf("Error reading body: %v", err)
        http.Error(w, "can't read body", http.StatusBadRequest)
        return
    }
    sum := filters.ComputeHash(body)
    fmt.Fprintf(w, "%s\n", sum)
    
}

func Download(w http.ResponseWriter, req *http.Request) {

    id := req.URL.Path[len(downloadEndpoint):]
    fmt.Fprintf(w, "Downloaded %s\n", id)
}

func SetupAndRun() {

    http.HandleFunc(uploadEndpoint, Upload)
    http.HandleFunc(downloadEndpoint, Download)

    http.ListenAndServe(":8090", nil)

}
