package cmd

import (
    "fmt"
    "net/http"
)

func upload(w http.ResponseWriter, req *http.Request) {

    fmt.Fprintf(w, "Uploaded %d bytes\n", req.ContentLength)
}

func download(w http.ResponseWriter, req *http.Request) {

    id := req.URL.Path[len("/download/"):]
    fmt.Fprintf(w, "Downloaded %s\n", id)
}

func SetupAndRun() {

    http.HandleFunc("/upload/", upload)
    http.HandleFunc("/download/", download)

    http.ListenAndServe(":8090", nil)

}
