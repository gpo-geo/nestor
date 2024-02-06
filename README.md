# nestor

File uploader web service, for training

Features implemented:

* Upload of files (of any size) by chunks
* Encryption before sending to a S3 bucket
* Download of files

TODO:

* Decryption on downloaded files
* Range download to get portions of data

## Setup requirement

* Launch a Minio instance on localhost:9000
* Create a bucket "gpo"
* Inject AWS credentials in environment variables

```
export AWS_REGION=us-west-2
export AWS_ENDPOINT_URL=localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
```

## Tests with tusd

This package implements the tus protocol to resume uploads. It allows to send large files by chunks,
and also provides a solution for network interuptions (no need to reupload the file from the start).


* "Creation" step: announce a new upload

```
curl -i -X POST \
  -H "Content-Length: 0" \
  -H "Upload-Length: 2000" \
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

To simplify later commands, you can export the identifier to a variable:

```
export TOKEN=d1b5a39052fb7a3ea5fb7c5f96c3530a
```


* "head" step: get current progress

```
curl -I -X HEAD \
  -H "Tus-Resumable: 1.0.0" \
  http://localhost:8080/files/$TOKEN
```

Returns the current offset.

* "patch" upload a first chunk

```
curl --range 0-999 -o part.dat file:///home/gpasero/coding/nestor/README.md
curl -i -X PATCH \
  -H "Content-Type: application/offset+octet-stream" \
  -H "Content-Length: 1000" \
  -H "Upload-Offset: 0" \
  -H "Tus-Resumable: 1.0.0" \
  --data-binary @part.dat \
  http://localhost:8080/files/$TOKEN
```

* "patch" upload a second chunk

```
curl --range 1000-1999 -o part.dat file:///home/gpasero/coding/nestor/README.md
curl -i -X PATCH \
  -H "Content-Type: application/offset+octet-stream" \
  -H "Content-Length: 1000" \
  -H "Upload-Offset: 1000" \
  -H "Tus-Resumable: 1.0.0" \
  --data-binary @part.dat \
  http://localhost:8080/files/$TOKEN
```

* "download" the complete file

```
curl -o output.dat http://localhost:8080/files/$TOKEN
```

## Design choice

The support of large file implies being able to upload it by piece. The [TUS](https://tus.io/) 
protocol was a simple solution for that. Some constraints are on the client side: the protocol is
specific, but compatible with cURL scripting and HTTP requests.

After checking the `tusd` server implementation in Go, it appear that it also support S3 storage
backend. I have looked into the implementation of `s3store` but there was too many private
functions and structs to efficiently reuse it.

For the implementation of encryption, I looked into three options: 

* Implement a new `S3API` interface that would implement encryption and feed it to `s3store`. I
  found that I would miss information to handle all cases. In particular, the upload of multi-part
  object can be done in parallel, but the encryption of the content has to be done sequencialy as I
  want to use a chained block cipher to avoid the issue: "My file is uploaded, but I need to know 
  how it was chunked to be able to decode it."

* Implement a new request handler to perform encryption/decryption on request body. I found that 
  it would also be difficult to handle all the cases, in particular the reuse of an incomplete part
  (potentially padded by encryption) that must be prepended to new data.
  
* Copy the implementation of `s3store`, rename, and make adaptations where needed. At least the
  `DataStore` and `Upload` interfaces needs to be implemented. The others should not be necessary. 
  This is certainly not the most elegant solution, but I was sure it would work. On the bright 
  side, I get a working code example and I can reuse a lot of the unit tests that were written
  for `s3store`. The tusd source code is released under MIT licence.

## About encryption

The encryption used is symmetric, with AES chained block cipher. The initial IV is the upload
identifier, which is an hexadecimal sequence corresponding to 16 bytes (size of the block cipher).

The encryption key is derived this way:

* The master key (a plain string sequence) is hashed with SHA-512
* The first half of the hash is used as encryption key (256 bits)
* The second half of the hash may be used in the future for authenticating with HMAC

During the upload process, the IV is stored in the ".info" file (as metadata). It is updated to
correspond to the last encoded block of the last multi-part object. This way, when the S3 concatenates
all the multi-part objects, we get the same encoded sequence as running the encryption in one go.

When dealing with incomplete parts, the content is also encrypted. When new data arrives, this 
incomplete part is downloaded, decrypted, then prepended to the new data before encryption. The
padding method used is PKCS, so that we can always retrieve the original message without padding.

