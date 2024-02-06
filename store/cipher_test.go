package store

import (
    "testing"
    "bytes"
    "crypto/aes"
    "crypto/cipher"
    "encoding/hex"
    "fmt"
)

func TestEncrypt(t *testing.T) {
    key, _ := hex.DecodeString("6368616e676520746869732070617373")
    plaintext := []byte("This is a secret that I don't want to share with the world wide web")
    
    length_before := len(plaintext)
    
    // apply padding
    plaintext, _ = PadBuffer(plaintext, aes.BlockSize)
    
    length_after := len(plaintext)
    if uint(plaintext[length_after-1]) != uint(length_after-length_before) {
        fmt.Printf("Padding value: expected %d, got %d\n", length_after-length_before, uint8(plaintext[length_after-1]))
        t.Fatal("Wrong padding value")
    }

    if len(plaintext)%aes.BlockSize != 0 {
        t.Fatal("plaintext is not a multiple of the block size")
    }
    
    block, err := aes.NewCipher(key)
    if err != nil {
        t.Fatal(err)
    }

    // The IV needs to be unique, but not secure. Therefore it's common to
    // include it at the beginning of the ciphertext. Here we expose the IV differently so no need
    // to prefix it in ciphertext
    iv, _ := hex.DecodeString("76e1976518c9209525161f85c1093290")
    ciphertext := make([]byte, len(plaintext))

    mode := cipher.NewCBCEncrypter(block, iv)
    mode.CryptBlocks(ciphertext, plaintext)

    // It's important to remember that ciphertexts must be authenticated
    // (i.e. by using crypto/hmac) as well as being encrypted in order to
    // be secure.

    //~ fmt.Printf("%x\n", ciphertext)
    
    // Try starting from second bloc
    secondtext := plaintext[aes.BlockSize:]
    next_iv := ciphertext[:aes.BlockSize]
    
    secondcipher := make([]byte, len(plaintext) - aes.BlockSize)
    
    secmode := cipher.NewCBCEncrypter(block, next_iv)
    secmode.CryptBlocks(secondcipher, secondtext)
    
    //~ fmt.Printf("%x\n", secondcipher)
    
    if ! bytes.Equal(ciphertext[aes.BlockSize:], secondcipher) {
        t.Fatal("Second cipher text is not the end of first cipher text")
    }
    
    // check with EncodeObject
    plaintext = []byte("This is a secret that I don't want to share with the world wide web")
    fakeReadCloser := bytes.NewReader(plaintext)
    otherciphertext, padSize, err := EncodeObject(fakeReadCloser, block, iv)
    
    if padSize != int(length_after-length_before) {
        t.Fatal("Wrong pad size")
    }
    
    if ! bytes.Equal(otherciphertext, ciphertext) {
        t.Fatal("Output of EncodeObject is different from previous cipher text")
    }
}

func TestDecrypt(t *testing.T) {
    key, _ := hex.DecodeString("6368616e676520746869732070617373")
    ciphertext, _ := hex.DecodeString("93a0c59482a10688ecc07eae98a690c504b4ff3767248fae7eb4238c51cd011059a0b9de602fdc4a305e7611b82a7c86f798460dfb1b10a74c490e0614739cc868f642061cd56b552b1f20c74b5581cf")
    iv, _ := hex.DecodeString("76e1976518c9209525161f85c1093290")
    
    plaintext := []byte("This is a secret that I don't want to share with the world wide web")
    
    original_length := len(plaintext)
    
    if len(ciphertext)%aes.BlockSize != 0 {
        t.Fatal("plaintext is not a multiple of the block size")
    }
    
    block, err := aes.NewCipher(key)
    if err != nil {
        t.Fatal(err)
    }
    
    result := make([]byte, len(ciphertext))
    
    mode := cipher.NewCBCDecrypter(block, iv)
    mode.CryptBlocks(result, ciphertext)
    
    if ! bytes.Equal(result[:original_length], plaintext) {
        t.Fatal("Decrypted and origin plaintext don't match")
    }
    
    // Partial decryption
    partialresult := make([]byte, 2 * aes.BlockSize)
    partialmode := cipher.NewCBCDecrypter(block, ciphertext[:aes.BlockSize])
    partialmode.CryptBlocks(partialresult, ciphertext[aes.BlockSize:3*aes.BlockSize])
    
    if ! bytes.Equal(partialresult, plaintext[aes.BlockSize:3*aes.BlockSize]) {
        t.Fatal("Partial decrypted message don't match with original")
    }
    
    // check with DecodeObject
    fakeReader := bytes.NewReader(ciphertext)
    otherresult, err := DecodeObject(fakeReader, int64(original_length), block, iv)
    if err != nil {
        t.Fatal(err)
    }
    
    if ! bytes.Equal(otherresult, plaintext) {
        fmt.Printf("'%s' vs '%s'", otherresult, plaintext)
        t.Fatal("Output of DecodeObject is different from original text")
    }
}


func TestDecodeAndTrim(t *testing.T) {
    key, _ := hex.DecodeString("6368616e676520746869732070617373")
    iv, _ := hex.DecodeString("76e1976518c9209525161f85c1093290")

    block, err := aes.NewCipher(key)
    if err != nil {
        t.Fatal(err)
    }
    
    // check with EncodeObject
    plaintext := []byte("This is a sec")
    myPlainReader := bytes.NewReader(plaintext)
    ciphertext, padSize, err := EncodePortableObject(myPlainReader, block, iv)
    
    if padSize != 3 {
        t.Fatal("Wrong pad size")
    }
    
    // check with DecodeObjectAndTrim
    cipherReader := bytes.NewReader(ciphertext)
    result, err := DecodeObjectAndTrim(cipherReader, block, iv)
    if err != nil {
        t.Fatal(err)
    }
    
    if ! bytes.Equal(result, plaintext) {
        fmt.Printf("'%s' vs '%s'", result, plaintext)
        t.Fatal("Output of DecodeObject is different from original text")
    }
}
