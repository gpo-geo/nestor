package handler

import (
    "context"
    "errors"
    //~ "fmt"
    "io"
    "net/http"
    "net/textproto"
    "strings"
    "strconv"
    "time"
    
    "github.com/gpo-geo/nestor/store"
)


type RangeDownloadHandler struct {
    Store *store.EncryptedStore
    Prefix string
}


func (h RangeDownloadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    
    if r.Method != "GET" {
        http.Error(w, "Use GET method", http.StatusMethodNotAllowed)
        return
    }
    byteRangeList, ok := r.Header["Range"]
    start := int64(0)
    length := int64(-1)
    var err error = nil
    if ok {
        start, length, err = parseRange(byteRangeList[0])
    }
    if err != nil {
        http.Error(w, "Wrong range", http.StatusBadRequest)
        return
    }

    // get context
    ctx := r.Context()
    
    // parse id
    id := strings.Trim(r.URL.Path, "/")
    index := strings.Index(id, "+")
    if index == -1 {
        http.Error(w, "Wrong requested id", http.StatusBadRequest)
        return
    }

    objectId := id[:index]
    multipartId := id[index+1:]

    // call RangeReader()
    out, err := h.Store.RangeReader(ctx, objectId, multipartId, start, length)
    if err != nil {
        http.Error(w, "Error while fetching the file: "+err.Error() , http.StatusInternalServerError)
        return
    }
    
    io.Copy(w, out)
    out.Close()
}

// parse a HTTP range, returns the start position and length. If start position is negative, it
// is relative to end of file. If length is negative, it means "until end of file"
func parseRange(s string) (int64, int64, error) {
    var range_start int64 = 0
    var range_length int64 = -1
    
    if s == "" {
        return range_start, range_length, nil
    }
    
    const b = "bytes="
    if !strings.HasPrefix(s, b) {
        return 0, 0, errors.New("invalid range")
    }
    
    if strings.Contains(s, ",") {
        return 0, 0, errors.New("multiple ranges not supported")
    }
    
    ra := textproto.TrimString(s[len(b):])
    if ra == "" {
        return 0, 0, errors.New("invalid range")
    }
    start, end, ok := strings.Cut(ra, "-")
    if !ok {
        return 0, 0, errors.New("invalid range")
    }
    if start == "" {
        // If no start is specified, end specifies the
        // range start relative to the end of the file,
        // and we are dealing with <suffix-length>
        // which has to be a non-negative integer as per
        // RFC 7233 Section 2.1 "Byte-Ranges".
        if end == "" || end[0] == '-' {
            return 0, 0, errors.New("invalid range")
        }
        i, err := strconv.ParseInt(end, 10, 64)
        if i < 0 || err != nil {
            return 0, 0, errors.New("invalid range")
        }
        range_start = -i
    } else {
        i, err := strconv.ParseInt(start, 10, 64)
        if err != nil || i < 0 {
            return 0, 0, errors.New("invalid range")
        }
        range_start = i
        // If no end is specified, range extends to end of the file.
        if end != "" {
            i, err := strconv.ParseInt(end, 10, 64)
            if err != nil || range_start > i {
                return 0, 0, errors.New("invalid range")
            }
            range_length = i - range_start + 1
        }
    }
    
    return range_start, range_length, nil
}
