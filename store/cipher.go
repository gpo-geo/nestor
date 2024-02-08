package store

import (
    "io"
    "errors"
    "crypto/cipher"
)

func RoundToNextMultiple(size int64, blockSize int64) int64 {
    remainder := size%blockSize
    if remainder != 0 {
        return size + blockSize - remainder
    }
    return size
}

// Apply padding to a buffer, given an input block size
// Returns the padded buffer and the number of bytes padded
func PadBuffer(buff []byte, blockSize int) ([]byte, int) {
    next_size := RoundToNextMultiple(int64(len(buff)), int64(blockSize))
    out := buff
    pad_length := int(next_size - int64(len(buff)))

    for i := 0; i < pad_length; i++ {
        out = append(out, uint8(pad_length))
    }

    return out, pad_length
}

func GetNextMultiple(size int64, blockSize int64) int64 {
    remainder := size%blockSize
    return size + blockSize - remainder
}

// Apply padding to a buffer, given an input block size
// Returns the padded buffer and the number of bytes padded
func PkcsPadBuffer(buff []byte, blockSize int) ([]byte, int) {
    next_size := GetNextMultiple(int64(len(buff)), int64(blockSize))
    out := buff
    pad_length := int(next_size - int64(len(buff)))
    
    if pad_length > 255 {
        panic("Padding larger than 255")
    }

    for i := 0; i < pad_length; i++ {
        out = append(out, uint8(pad_length))
    }

    return out, pad_length
}

// Encode the input data from a ReadCloser, using a given block cipher and an IV. 
// Returns the encoded data, and the number of padding bytes
func EncodeObject(src io.Reader, block cipher.Block, iv []byte) ([]byte, int, error ) {
    
    // TODO : implement a ReadCloser wrapper for this
    inBuffer, err := io.ReadAll(src)
    if err != nil {
		return nil, 0, err
	}

    padSize := 0
    inBuffer, padSize = PadBuffer(inBuffer, block.BlockSize())

    mode := cipher.NewCBCEncrypter(block, iv)
    mode.CryptBlocks(inBuffer, inBuffer)

    return inBuffer, padSize, nil
}

// Encode the input data from a ReadCloser, using a given block cipher and an IV. 
// Returns the encoded data, and the number of padding bytes
func EncodePortableObject(src io.Reader, block cipher.Block, iv []byte) ([]byte, int, error ) {
    
    // TODO : implement a ReadCloser wrapper for this
    inBuffer, err := io.ReadAll(src)
    if err != nil {
		return nil, 0, err
	}

    padSize := 0
    inBuffer, padSize = PkcsPadBuffer(inBuffer, block.BlockSize())

    mode := cipher.NewCBCEncrypter(block, iv)
    mode.CryptBlocks(inBuffer, inBuffer)

    return inBuffer, padSize, nil
}


// Decode the input data from a ReadCloser, using a given block cipher and an IV.
// Returns the decoded data
func DecodeObject(src io.Reader, block cipher.Block, iv []byte) ([]byte, error ) {

    inBuffer, err := io.ReadAll(src)
    if err != nil {
		return nil, err
	}

    mode := cipher.NewCBCDecrypter(block, iv)
    mode.CryptBlocks(inBuffer, inBuffer)

    return inBuffer, nil
}


// Decode the input data from a Reader, using a given block cipher and an IV. The last decoded 
// block is checked to derive the padding value
// Returns the decoded data
func DecodeObjectAndTrim(src io.Reader, block cipher.Block, iv []byte) ([]byte, error ) {

    inBuffer, err := io.ReadAll(src)
    if err != nil {
		return nil, err
	}
    
    encodedLength := len(inBuffer)
    
    // TODO : implement a Reader wrapper for this
    mode := cipher.NewCBCDecrypter(block, iv)
    mode.CryptBlocks(inBuffer, inBuffer)
    
    padSize := uint8(inBuffer[encodedLength-1])
    
    if padSize == 0 || int(padSize) > block.BlockSize() {
        // wrong padding value
        return nil, errors.New("Decoding failed")
    }

    for i := 1; i < int(padSize); i++ {
        if uint8(inBuffer[encodedLength - 1 - i]) != padSize {
            return nil, errors.New("Decoding failed")
        }
    }

    return inBuffer[:encodedLength-int(padSize)], nil
}
