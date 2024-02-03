package store

func PadBuffer(buff []byte, blockSize int) ([]byte) {
    remainder := len(buff)%blockSize
    out := buff
    if remainder != 0 {
        pad_length := blockSize - remainder
        for i := 0; i < pad_length; i++ {
            out = append(out, uint8(pad_length))
        }
    }
    return out
}
