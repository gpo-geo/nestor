package filters

import (
    "fmt"
    "golang.org/x/crypto/blake2b"
)


func ComputeHash(data []byte) string {
    sum := blake2b.Sum256(data)
    
    return fmt.Sprintf("%x", sum)
}
