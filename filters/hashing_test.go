package filters

import (
    "testing"
)


func TestHashString(t *testing.T) {
    buf := []byte("This is a test")

    hash := ComputeHash(buf)
    expected := "fe9bee7df50a35fda7d9175add85b1feaf615607bfb15d629ff2103218c63e62"
    
    if hash != expected {
        t.Fatalf(`ComputeHash("This is a test") = %s, want %s, error`, hash, expected)
    }
}
