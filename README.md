# nestor

File uploader web service, for training

## Tests with tusd

This package implements the tus protocol to resume uploads. It allows to send large files by chunks,
and also provides a solution for network interuptions (no need to reupload the file from the start).

Some constrains on client side: the protocol is specific, but compatible with cURL scripting.

* "Creation" step: announce a new upload

```
curl -i -X POST \
  -H "Content-Length: 0" \
  -H "Upload-Length: 49" \
  -H "Tus-Resumable: 1.0.0" \
  -H "Upload-Metadata: key value, filename README.md" \
  http://localhost:8080/files/
```
  
Returns the upload identifier, example: 

```
HTTP/1.1 201 Created
Location: http://localhost:8080/files/d1b5a39052fb7a3ea5fb7c5f96c3530a
Tus-Resumable: 1.0.0
X-Content-Type-Options: nosniff
Date: Wed, 31 Jan 2024 22:05:48 GMT
Content-Length: 0
```

* "head" step: get current progress

```
curl -I -X HEAD \
  -H "Tus-Resumable: 1.0.0" \
  http://localhost:8080/files/$TOKEN
```

Returns the current offset.

* "patch" upload a new patch

```
curl --range 10-19 -o part.dat file:///home/gpasero/coding/nestor/README.md
curl -i -X PATCH \
  -H "Content-Type: application/offset+octet-stream" \
  -H "Content-Length: 10" \
  -H "Upload-Offset: 0" \
  -H "Tus-Resumable: 1.0.0" \
  --data-binary @part.dat \
  http://localhost:8080/files/$TOKEN
```

* "download" the complete file

```
curl -o output.dat http://localhost:8080/files/$TOKEN
```


## Test with encryption

encoding a message, with secret `champignon`:

```
openssl enc -aes256 -base64 -pass pass:champignon -pbkdf2 -in input.txt -out input.enc
```

decoding :

```
openssl enc -d -aes256 -base64 -pass pass:champignon -pbkdf2 -in input.enc -out input.dec
```

Notes:

* encrypted content is not identic to input message length.
* the encryption function is not associative: enc(A+B) != enc(A) + enc(B)

* Idea to integrate : make a custom [S3Store](https://pkg.go.dev/github.com/tus/tusd/v2@v2.2.2/pkg/s3store#S3Store) ?
  or wrapping an existing store ?
