package cmd

import (
    "github.com/gpo-geo/nestor/filters"
    "fmt"
    "io/ioutil"
    "net/http"
    "log"
)

func Upload(w http.ResponseWriter, req *http.Request) {

    if req.Method != "POST" {
        http.Error(w, "Use POST method", http.StatusBadRequest)
        return
    }

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

    id := req.URL.Path[len("/download/"):]
    fmt.Fprintf(w, "Downloaded %s\n", id)
}

func SetupAndRun() {

    http.HandleFunc("/upload/", Upload)
    http.HandleFunc("/download/", Download)

    http.ListenAndServe(":8090", nil)

}
