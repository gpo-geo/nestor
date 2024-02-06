package store

import (
    "context"
    "crypto/rand"
    "crypto/aes"
    "crypto/cipher"
    "crypto/sha256"
    "crypto/sha512"
    "github.com/tus/tusd/v2/pkg/handler"
    "github.com/tus/tusd/v2/pkg/s3store"
    //~ "github.com/aws/aws-sdk-go-v2/config"
    //~ "github.com/aws/aws-sdk-go-v2/service/s3"
    "bytes"
	//~ "context"
	"encoding/json"
    "encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	//~ "github.com/prometheus/client_golang/prometheus"
	//~ "github.com/tus/tusd/v2/internal/semaphore"
	//~ "github.com/tus/tusd/v2/internal/uid"
	//~ "github.com/tus/tusd/v2/pkg/handler"
	"golang.org/x/exp/slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)


// This regular expression matches every character which is not
// considered valid into a header value according to RFC2616.
var nonPrintableRegexp = regexp.MustCompile(`[^\x09\x20-\x7E]`)

// ------------------------------------------------------------------------------------------------

type EncryptedStore struct {
    s3store.S3Store
    block cipher.Block
    // TODO: hmac
    
    // uploadSemaphore limits the number of concurrent multipart part uploads to S3.
	uploadSemaphore Semaphore

	//~ // requestDurationMetric holds the prometheus instance for storing the request durations.
	//~ requestDurationMetric *prometheus.SummaryVec

	//~ // diskWriteDurationMetric holds the prometheus instance for storing the time it takes to write chunks to disk.
	//~ diskWriteDurationMetric prometheus.Summary

	//~ // uploadSemaphoreDemandMetric holds the prometheus instance for storing the demand on the upload semaphore
	//~ uploadSemaphoreDemandMetric prometheus.Gauge

	//~ // uploadSemaphoreLimitMetric holds the prometheus instance for storing the limit on the upload semaphore
	//~ uploadSemaphoreLimitMetric prometheus.Gauge
}

func New(bucket string, service s3store.S3API, key string) EncryptedStore {
    // hash the master key with SHA512, take the first half for encryption key, second half may
    // be used for hmac
    key_bytes := []byte(key)
    hashed_key := sha512.Sum512(key_bytes)
    block, err := aes.NewCipher(hashed_key[:32])
    if err != nil {
        panic(err)
    }

    child := s3store.New(bucket, service)

    output := EncryptedStore{
        child,
        block,
        NewSemaphore(10),
    }

    return output
}

// ------------------------------------------------------------------------------------------------


// The labels to use for observing and storing request duration. One label per operation.
const (
	metricGetInfoObject           = "get_info_object"
	metricPutInfoObject           = "put_info_object"
	metricCreateMultipartUpload   = "create_multipart_upload"
	metricCompleteMultipartUpload = "complete_multipart_upload"
	metricUploadPart              = "upload_part"
	metricListParts               = "list_parts"
	metricHeadPartObject          = "head_part_object"
	metricGetPartObject           = "get_part_object"
	metricPutPartObject           = "put_part_object"
	metricDeletePartObject        = "delete_part_object"
)




// SetConcurrentPartUploads changes the limit on how many concurrent part uploads to S3 are allowed.
//~ func (store *EncryptedStore) SetConcurrentPartUploads(limit int) {
	//~ store.uploadSemaphore = NewSemaphore(limit)
	//~ store.uploadSemaphoreLimitMetric.Set(float64(limit))
//~ }

// UseIn sets this store as the core data store in the passed composer and adds
// all possible extension to it.
func (store EncryptedStore) UseIn(composer *handler.StoreComposer) {
	composer.UseCore(store)
	//~ composer.UseTerminater(store)
	//~ composer.UseConcater(store)
	//~ composer.UseLengthDeferrer(store)
}

//~ func (store EncryptedStore) RegisterMetrics(registry prometheus.Registerer) {
	//~ registry.MustRegister(store.requestDurationMetric)
	//~ registry.MustRegister(store.diskWriteDurationMetric)
	//~ registry.MustRegister(store.uploadSemaphoreDemandMetric)
	//~ registry.MustRegister(store.uploadSemaphoreLimitMetric)
//~ }

func (store EncryptedStore) observeRequestDuration(start time.Time, label string) {
	elapsed := time.Since(start)
	ms := float64(elapsed.Nanoseconds() / int64(time.Millisecond))

    if false {
        fmt.Printf("DEBUG: %s : %f ms\n", label, ms)
    }
	//~ store.requestDurationMetric.WithLabelValues(label).Observe(ms)
}

type cryptUpload struct {
	// objectId is the object key under which we save the final file
	objectId string
	// multipartId is the ID given by S3 to us for the multipart upload
	multipartId string

	store *EncryptedStore

	// info stores the upload's current FileInfo struct. It may be nil if it hasn't
	// been fetched yet from S3. Never read or write to it directly but instead use
	// the GetInfo and writeInfo functions.
	info *handler.FileInfo

	// parts collects all parts for this upload. It will be nil if info is nil as well.
	parts []*s3Part
	// incompletePartSize is the size of an incomplete part object, if one exists. It will be 0 if info is nil as well.
	incompletePartSize int64
}

// s3Part represents a single part of a S3 multipart upload.
type s3Part struct {
	number int32
	size   int64
	etag   string
}

func (store EncryptedStore) NewUpload(ctx context.Context, info handler.FileInfo) (handler.Upload, error) {
	// an upload larger than MaxObjectSize must throw an error
	if info.Size > store.MaxObjectSize {
		return nil, fmt.Errorf("s3store: upload size of %v bytes exceeds MaxObjectSize of %v bytes", info.Size, store.MaxObjectSize)
	}

	var objectId string
    var iv string
	if info.ID == "" {
		objectId = Uid()
        iv = objectId
	} else {
		// certain tests set info.ID in advance
		objectId = info.ID
        objBytes := []byte(objectId)
        objHashed := sha256.Sum256(objBytes)
        iv = hex.EncodeToString(objHashed[:store.block.BlockSize()])
	}
    
    // store initial IV in metadata
    if info.MetaData == nil {
        info.MetaData = make(map[string]string, 1)
    }
    info.MetaData["IV"] = iv

	// Convert meta data into a map of pointers for AWS Go SDK, sigh.
	metadata := make(map[string]string, len(info.MetaData))
	for key, value := range info.MetaData {
		metadata[key] = nonPrintableRegexp.ReplaceAllString(value, "?")
	}

	// Create the actual multipart upload
	t := time.Now()
	res, err := store.Service.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(store.Bucket),
		Key:      store.keyWithPrefix(objectId),
		Metadata: metadata,
	})
	store.observeRequestDuration(t, metricCreateMultipartUpload)
	if err != nil {
		return nil, fmt.Errorf("s3store: unable to create multipart upload:\n%s", err)
	}

	multipartId := *res.UploadId
	info.ID = objectId + "+" + multipartId

	info.Storage = map[string]string{
		"Type":   "s3store",
		"Bucket": store.Bucket,
		"Key":    *store.keyWithPrefix(objectId),
	}

	upload := &cryptUpload{objectId, multipartId, &store, nil, []*s3Part{}, 0}
	err = upload.writeInfo(ctx, info)
	if err != nil {
		return nil, fmt.Errorf("s3store: unable to create info file:\n%s", err)
	}

	return upload, nil
}

func (store EncryptedStore) GetUpload(ctx context.Context, id string) (handler.Upload, error) {
	objectId, multipartId := splitIds(id)
	if objectId == "" || multipartId == "" {
		// If one of them is empty, it cannot be a valid ID.
		return nil, handler.ErrNotFound
	}

	return &cryptUpload{objectId, multipartId, &store, nil, []*s3Part{}, 0}, nil
}

//~ func (store S3Store) AsTerminatableUpload(upload handler.Upload) handler.TerminatableUpload {
	//~ return upload.(*cryptUpload)
//~ }

//~ func (store S3Store) AsLengthDeclarableUpload(upload handler.Upload) handler.LengthDeclarableUpload {
	//~ return upload.(*cryptUpload)
//~ }

//~ func (store S3Store) AsConcatableUpload(upload handler.Upload) handler.ConcatableUpload {
	//~ return upload.(*cryptUpload)
//~ }

func (upload *cryptUpload) writeInfo(ctx context.Context, info handler.FileInfo) error {
	store := upload.store

	upload.info = &info

	infoJson, err := json.Marshal(info)

	if err != nil {
		return err
	}

	// Create object on S3 containing information about the file
	t := time.Now()
	_, err = store.Service.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(store.Bucket),
		Key:           store.metadataKeyWithPrefix(upload.objectId + ".info"),
		Body:          bytes.NewReader(infoJson),
		ContentLength: aws.Int64(int64(len(infoJson))),
	})
	store.observeRequestDuration(t, metricPutInfoObject)

	return err
}

func (upload *cryptUpload) WriteChunk(ctx context.Context, offset int64, src io.Reader) (int64, error) {
	store := upload.store

	// Get the total size of the current upload, number of parts to generate next number and whether
	// an incomplete part exists
	info, _, incompletePartSize, err := upload.getInternalInfo(ctx)
	if err != nil {
		return 0, err
	}
    
    // get IV from uid or last fully encoded part
    iv, err := hex.DecodeString(info.MetaData["IV"])

    if err != nil {
		return 0, err
	}

	if incompletePartSize > 0 {
		incompletePartFile, err := store.downloadIncompletePartForUpload(ctx, upload.objectId, incompletePartSize, iv)
		if err != nil {
			return 0, err
		}
		if incompletePartFile == nil {
			return 0, fmt.Errorf("s3store: Expected an incomplete part file but did not get any")
		}
		defer cleanUpTempFile(incompletePartFile)

		if err := store.deleteIncompletePartForUpload(ctx, upload.objectId); err != nil {
			return 0, err
		}

		// Prepend an incomplete part, if necessary and adapt the offset
		src = io.MultiReader(incompletePartFile, src)
		offset = offset - incompletePartSize
	}

	bytesUploaded, err := upload.uploadParts(ctx, offset, src, iv)

	// The size of the incomplete part should not be counted, because the
	// process of the incomplete part should be fully transparent to the user.
	bytesUploaded = bytesUploaded - incompletePartSize
	if bytesUploaded < 0 {
		bytesUploaded = 0
	}

	upload.info.Offset += bytesUploaded

    if err == nil {
        // the IV in the metadata needs an update
        info.MetaData["IV"] = upload.info.MetaData["IV"] // hex.EncodeToString(last_iv)

        infoUdateErr := upload.writeInfo(ctx, info)
        if infoUdateErr != nil {
            err = infoUdateErr
        }
    }

	return bytesUploaded, err
}

func (upload *cryptUpload) uploadParts(ctx context.Context, offset int64, src io.Reader, iv []byte) (int64, error) {
	store := upload.store

	// Get the total size of the current upload and number of parts to generate next number
	info, parts, _, err := upload.getInternalInfo(ctx)
	if err != nil {
		return 0, err
	}

	size := info.Size
	bytesUploaded := int64(0)
	optimalPartSize, err := store.calcOptimalPartSize(size)
	if err != nil {
		return 0, err
	}
    blocksize := int64(store.block.BlockSize())

	numParts := len(parts)
	nextPartNum := int32(numParts + 1)

    // Encode the source data
    buffer, padSize, err := EncodePortableObject(src, store.block, iv)
    originalSize := len(buffer) - padSize
    // check corner case: padSize == blocksize and originalSize % optimalPartSize == 0
    // in this case, we can ignore the last block which would give an incomplete file with only padding
    if int64(padSize) == blocksize && (int64(originalSize) % optimalPartSize == 0) {
        buffer = buffer[:originalSize]
    }

    bufferReader := bytes.NewReader(buffer)

	partProducer, fileChan := newS3PartProducer(bufferReader, store.MaxBufferedParts, store.TemporaryDirectory)

	producerCtx, cancelProducer := context.WithCancel(ctx)
	defer func() {
		cancelProducer()
		partProducer.closeUnreadFiles()
	}()
	go partProducer.produce(producerCtx, optimalPartSize)

	var wg sync.WaitGroup
	var uploadErr error

	for {
		// We acquire the semaphore before starting the goroutine to avoid
		// starting many goroutines, most of which are just waiting for the lock.
		// We also acquire the semaphore before reading from the channel to reduce
		// the number of part files are laying around on disk without being used.
		upload.store.acquireUploadSemaphore()
		fileChunk, more := <-fileChan
		if !more {
			upload.store.releaseUploadSemaphore()
			break
		}

		partfile := fileChunk.reader
		partsize := fileChunk.size
		closePart := fileChunk.closeReader
        
        // the condition "partsize >= store.MinPartSize" is not be adapted if the MinPartSize is equal to blocksize
        // We need to check the partsize without padding
        partSizeNoPad := partsize
        if (bytesUploaded+partsize) > int64(originalSize) {
            partSizeNoPad -= int64(padSize)
        }

		isFinalChunk := !info.SizeIsDeferred && (size == offset+bytesUploaded+partSizeNoPad)
        
		if partSizeNoPad >= store.MinPartSize || isFinalChunk {
			part := &s3Part{
				etag:   "",
				size:   partsize,
				number: nextPartNum,
			}
			upload.parts = append(upload.parts, part)

			wg.Add(1)
			go func(file io.ReadSeeker, part *s3Part, closePart func() error) {
				defer upload.store.releaseUploadSemaphore()
				defer wg.Done()

				t := time.Now()
				uploadPartInput := &s3.UploadPartInput{
					Bucket:     aws.String(store.Bucket),
					Key:        store.keyWithPrefix(upload.objectId),
					UploadId:   aws.String(upload.multipartId),
					PartNumber: aws.Int32(part.number),
				}
				etag, err := upload.putPartForUpload(ctx, uploadPartInput, file, part.size)
				store.observeRequestDuration(t, metricUploadPart)
				if err != nil {
					uploadErr = err
				} else {
					part.etag = etag
				}
				if cerr := closePart(); cerr != nil && uploadErr == nil {
					uploadErr = cerr
				}
			}(partfile, part, closePart)
		} else {
			wg.Add(1)
			go func(file io.ReadSeeker, closePart func() error) {
				defer upload.store.releaseUploadSemaphore()
				defer wg.Done()

				if err := store.putIncompletePartForUpload(ctx, upload.objectId, file); err != nil {
					uploadErr = err
				}
				if cerr := closePart(); cerr != nil && uploadErr == nil {
					uploadErr = cerr
				}
				upload.incompletePartSize = partSizeNoPad
			}(partfile, closePart)
		}

		bytesUploaded += partSizeNoPad
		nextPartNum += 1
	}

	wg.Wait()

	if uploadErr != nil {
		return 0, uploadErr
	}
    
    // read the latest IV
    next_iv := iv
    if bytesUploaded >= optimalPartSize {
        bytesToLastPart := bytesUploaded
        remainder := bytesUploaded % optimalPartSize
        if remainder != 0 {
            bytesToLastPart -= remainder
        }
        next_iv = buffer[(bytesToLastPart-blocksize):bytesToLastPart]
    }
    upload.info.MetaData["IV"] = hex.EncodeToString(next_iv)

	return bytesUploaded, partProducer.err
}

func cleanUpTempFile(file *os.File) {
	file.Close()
	os.Remove(file.Name())
}

func (upload *cryptUpload) putPartForUpload(ctx context.Context, uploadPartInput *s3.UploadPartInput, file io.ReadSeeker, size int64) (string, error) {
	if !upload.store.DisableContentHashes {
		// By default, use the traditional approach to upload data
		uploadPartInput.Body = file
		res, err := upload.store.Service.UploadPart(ctx, uploadPartInput)
		if err != nil {
			return "", err
		}
		return *res.ETag, nil
	} else {
		// Experimental feature to prevent the AWS SDK from calculating the SHA256 hash
		// for the parts we upload to S3.
		// We compute the presigned URL without the body attached and then send the request
		// on our own. This way, the body is not included in the SHA256 calculation.
		s3Client, ok := upload.store.Service.(*s3.Client)
		if !ok {
			return "", fmt.Errorf("s3store: failed to cast S3 service for presigning")
		}

		presignClient := s3.NewPresignClient(s3Client)

		s3Req, err := presignClient.PresignUploadPart(ctx, uploadPartInput, func(opts *s3.PresignOptions) {
			opts.Expires = 15 * time.Minute
		})
		if err != nil {
			return "", fmt.Errorf("s3store: failed to presign UploadPart: %s", err)
		}

		req, err := http.NewRequest("PUT", s3Req.URL, file)
		if err != nil {
			return "", err
		}

		// Set the Content-Length manually to prevent the usage of Transfer-Encoding: chunked,
		// which is not supported by AWS S3.
		req.ContentLength = size

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			buf := new(strings.Builder)
			io.Copy(buf, res.Body)
			return "", fmt.Errorf("s3store: unexpected response code %d for presigned upload: %s", res.StatusCode, buf.String())
		}

		return res.Header.Get("ETag"), nil
	}
}

func (upload *cryptUpload) GetInfo(ctx context.Context) (info handler.FileInfo, err error) {
	info, _, _, err = upload.getInternalInfo(ctx)
	return info, err
}

func (upload *cryptUpload) getInternalInfo(ctx context.Context) (info handler.FileInfo, parts []*s3Part, incompletePartSize int64, err error) {
	if upload.info != nil {
		return *upload.info, upload.parts, upload.incompletePartSize, nil
	}

	info, parts, incompletePartSize, err = upload.fetchInfo(ctx)
	if err != nil {
		return info, parts, incompletePartSize, err
	}

	upload.info = &info
	upload.parts = parts
	upload.incompletePartSize = incompletePartSize
	return info, parts, incompletePartSize, nil
}

func (upload cryptUpload) fetchInfo(ctx context.Context) (info handler.FileInfo, parts []*s3Part, incompletePartSize int64, err error) {
	store := upload.store

	var wg sync.WaitGroup
	wg.Add(3)

	// We store all errors in here and handle them all together once the wait
	// group is done.
	var infoErr error
	var partsErr error
	var incompletePartSizeErr error

	go func() {
		defer wg.Done()
		t := time.Now()

		// Get file info stored in separate object
		var res *s3.GetObjectOutput
		res, infoErr = store.Service.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(store.Bucket),
			Key:    store.metadataKeyWithPrefix(upload.objectId + ".info"),
		})
		store.observeRequestDuration(t, metricGetInfoObject)
		if infoErr == nil {
			infoErr = json.NewDecoder(res.Body).Decode(&info)
		}
	}()

	go func() {
		defer wg.Done()

		// Get uploaded parts and their offset
		parts, partsErr = store.listAllParts(ctx, upload.objectId, upload.multipartId)
	}()

	go func() {
		defer wg.Done()

		// Get size of optional incomplete part file.
		incompletePartSize, incompletePartSizeErr = store.headIncompletePartForUpload(ctx, upload.objectId)
	}()

	wg.Wait()

	// Finally, after all requests are complete, let's handle the errors
	if infoErr != nil {
		err = infoErr
		// If the info file is not found, we consider the upload to be non-existant
		if isAwsError[*types.NoSuchKey](err) {
			err = handler.ErrNotFound
		}
		return
	}

	if partsErr != nil {
		err = partsErr
		// Check if the error is caused by the multipart upload not being found. This happens
		// when the multipart upload has already been completed or aborted. Since
		// we already found the info object, we know that the upload has been
		// completed and therefore can ensure the the offset is the size.
		// AWS S3 returns NoSuchUpload, but other implementations, such as DigitalOcean
		// Spaces, can also return NoSuchKey.

		// The AWS Go SDK v2 has a bug where types.NoSuchUpload is not returned,
		// so we also need to check the error code itself.
		// See https://github.com/aws/aws-sdk-go-v2/issues/1635
		// In addition, S3-compatible storages, like DigitalOcean Spaces, might cause
		// types.NoSuchKey to not be returned as well.
		if isAwsError[*types.NoSuchUpload](err) || isAwsErrorCode(err, "NoSuchUpload") || isAwsError[*types.NoSuchKey](err) || isAwsErrorCode(err, "NoSuchKey") {
			info.Offset = info.Size
			err = nil
		}
		return
	}

	if incompletePartSizeErr != nil {
		err = incompletePartSizeErr
		return
	}
    
    // Refine the incompletePartSize: get the padding value
    if incompletePartSize > 0 {
        tailSize := 2 * store.block.BlockSize()
        if incompletePartSize < int64(tailSize) {
            tailSize = store.block.BlockSize()
        }
        byteRangeString := fmt.Sprintf("bytes=-%d", tailSize)
        var incompleteTail *s3.GetObjectOutput
        incompleteTail, incompletePartSizeErr = store.getIncompletePartForUpload(ctx, upload.objectId, &byteRangeString)
        if incompletePartSizeErr != nil {
            err = incompletePartSizeErr
            return
        }
        // Decode the last portion
        var iv []byte
        var ivErr error
        if tailSize == store.block.BlockSize() {
            iv, ivErr = hex.DecodeString(info.MetaData["IV"])
            if ivErr != nil {
                err = ivErr
                return
            }
        } else {
            iv = make([]byte, store.block.BlockSize())
            _, ivErr = incompleteTail.Body.Read(iv)
            if ivErr != nil {
                err = ivErr
                return
            }
        }
        decodedTail, decodeErr := DecodeObjectAndTrim(incompleteTail.Body, store.block, iv)
        if decodeErr != nil {
            err = decodeErr
            return
        }
        padSize := store.block.BlockSize() - len(decodedTail)
        incompletePartSize = incompletePartSize - int64(padSize)
    }

	// The offset is the sum of all part sizes and the size of the incomplete part file.
	offset := incompletePartSize
	for _, part := range parts {
		offset += part.size
	}
    info.Offset = offset

	return info, parts, incompletePartSize, nil
}

func (upload cryptUpload) GetReader(ctx context.Context) (io.ReadCloser, error) {
	store := upload.store

	// Attempt to get upload content
	res, err := store.Service.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(store.Bucket),
		Key:    store.keyWithPrefix(upload.objectId),
	})
	if err == nil {
		// No error occurred, and we are able to stream the object
		return res.Body, nil
	}

	// If the file cannot be found, we ignore this error and continue since the
	// upload may not have been finished yet. In this case we do not want to
	// return a ErrNotFound but a more meaning-full message.
	if !isAwsError[*types.NoSuchKey](err) {
		return nil, err
	}

	// Test whether the multipart upload exists to find out if the upload
	// never existsted or just has not been finished yet
	_, err = store.Service.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String(store.Bucket),
		Key:      store.keyWithPrefix(upload.objectId),
		UploadId: aws.String(upload.multipartId),
		MaxParts: aws.Int32(0),
	})
	if err == nil {
		// The multipart upload still exists, which means we cannot download it yet
		return nil, handler.NewError("ERR_INCOMPLETE_UPLOAD", "cannot stream non-finished upload", http.StatusBadRequest)
	}

	// The AWS Go SDK v2 has a bug where types.NoSuchUpload is not returned,
	// so we also need to check the error code itself.
	// See https://github.com/aws/aws-sdk-go-v2/issues/1635
	if isAwsError[*types.NoSuchUpload](err) || isAwsErrorCode(err, "NoSuchUpload") {
		// Neither the object nor the multipart upload exists, so we return a 404
		return nil, handler.ErrNotFound
	}

	return nil, err
}

//~ func (upload cryptUpload) Terminate(ctx context.Context) error {
	//~ store := upload.store

	//~ var wg sync.WaitGroup
	//~ wg.Add(2)
	//~ errs := make([]error, 0, 3)

	//~ go func() {
		//~ defer wg.Done()

		//~ // Abort the multipart upload
		//~ _, err := store.Service.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			//~ Bucket:   aws.String(store.Bucket),
			//~ Key:      store.keyWithPrefix(upload.objectId),
			//~ UploadId: aws.String(upload.multipartId),
		//~ })
		//~ if err != nil && !isAwsError[*types.NoSuchUpload](err) {
			//~ errs = append(errs, err)
		//~ }
	//~ }()

	//~ go func() {
		//~ defer wg.Done()

		//~ // Delete the info and content files
		//~ res, err := store.Service.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			//~ Bucket: aws.String(store.Bucket),
			//~ Delete: &types.Delete{
				//~ Objects: []types.ObjectIdentifier{
					//~ {
						//~ Key: store.keyWithPrefix(upload.objectId),
					//~ },
					//~ {
						//~ Key: store.metadataKeyWithPrefix(upload.objectId + ".part"),
					//~ },
					//~ {
						//~ Key: store.metadataKeyWithPrefix(upload.objectId + ".info"),
					//~ },
				//~ },
				//~ Quiet: aws.Bool(true),
			//~ },
		//~ })

		//~ if err != nil {
			//~ errs = append(errs, err)
			//~ return
		//~ }

		//~ for _, s3Err := range res.Errors {
			//~ if *s3Err.Code != "NoSuchKey" {
				//~ errs = append(errs, fmt.Errorf("AWS S3 Error (%s) for object %s: %s", *s3Err.Code, *s3Err.Key, *s3Err.Message))
			//~ }
		//~ }
	//~ }()

	//~ wg.Wait()

	//~ if len(errs) > 0 {
		//~ return newMultiError(errs)
	//~ }

	//~ return nil
//~ }

func (upload cryptUpload) FinishUpload(ctx context.Context) error {
	store := upload.store

	// Get uploaded parts
	_, parts, _, err := upload.getInternalInfo(ctx)
	if err != nil {
		return err
	}

	if len(parts) == 0 {
		// AWS expects at least one part to be present when completing the multipart
		// upload. So if the tus upload has a size of 0, we create an empty part
		// and use that for completing the multipart upload.
		res, err := store.Service.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(store.Bucket),
			Key:        store.keyWithPrefix(upload.objectId),
			UploadId:   aws.String(upload.multipartId),
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader([]byte{}),
		})
		if err != nil {
			return err
		}

		parts = []*s3Part{
			{
				etag:   *res.ETag,
				number: 1,
				size:   0,
			},
		}

	}

	// Transform the []*s3.Part slice to a []*s3.CompletedPart slice for the next
	// request.
	completedParts := make([]types.CompletedPart, len(parts))

	for index, part := range parts {
		completedParts[index] = types.CompletedPart{
			ETag:       aws.String(part.etag),
			PartNumber: aws.Int32(part.number),
		}
	}

	t := time.Now()
	_, err = store.Service.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(store.Bucket),
		Key:      store.keyWithPrefix(upload.objectId),
		UploadId: aws.String(upload.multipartId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	store.observeRequestDuration(t, metricCompleteMultipartUpload)

	return err
}

//~ func (upload *cryptUpload) ConcatUploads(ctx context.Context, partialUploads []handler.Upload) error {
	//~ hasSmallPart := false
	//~ for _, partialUpload := range partialUploads {
		//~ info, err := partialUpload.GetInfo(ctx)
		//~ if err != nil {
			//~ return err
		//~ }

		//~ if info.Size < upload.store.MinPartSize {
			//~ hasSmallPart = true
		//~ }
	//~ }

	//~ // If one partial upload is smaller than the the minimum part size for an S3
	//~ // Multipart Upload, we cannot use S3 Multipart Uploads for concatenating all
	//~ // the files.
	//~ // So instead we have to download them and concat them on disk.
	//~ if hasSmallPart {
		//~ return upload.concatUsingDownload(ctx, partialUploads)
	//~ } else {
		//~ return upload.concatUsingMultipart(ctx, partialUploads)
	//~ }
//~ }

//~ func (upload *cryptUpload) concatUsingDownload(ctx context.Context, partialUploads []handler.Upload) error {
	//~ store := upload.store

	//~ // Create a temporary file for holding the concatenated data
	//~ file, err := os.CreateTemp(store.TemporaryDirectory, "tusd-s3-concat-tmp-")
	//~ if err != nil {
		//~ return err
	//~ }
	//~ defer cleanUpTempFile(file)

	//~ // Download each part and append it to the temporary file
	//~ for _, partialUpload := range partialUploads {
		//~ partialS3Upload := partialUpload.(*cryptUpload)

		//~ res, err := store.Service.GetObject(ctx, &s3.GetObjectInput{
			//~ Bucket: aws.String(store.Bucket),
			//~ Key:    store.keyWithPrefix(partialS3Upload.objectId),
		//~ })
		//~ if err != nil {
			//~ return err
		//~ }
		//~ defer res.Body.Close()

		//~ if _, err := io.Copy(file, res.Body); err != nil {
			//~ return err
		//~ }
	//~ }

	//~ // Seek to the beginning of the file, so the entire file is being uploaded
	//~ file.Seek(0, 0)

	//~ // Upload the entire file to S3
	//~ _, err = store.Service.PutObject(ctx, &s3.PutObjectInput{
		//~ Bucket: aws.String(store.Bucket),
		//~ Key:    store.keyWithPrefix(upload.objectId),
		//~ Body:   file,
	//~ })
	//~ if err != nil {
		//~ return err
	//~ }

	//~ // Finally, abort the multipart upload since it will no longer be used.
	//~ // This happens asynchronously since we do not need to wait for the result.
	//~ // Also, the error is ignored on purpose as it does not change the outcome of
	//~ // the request.
	//~ go func() {
		//~ store.Service.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			//~ Bucket:   aws.String(store.Bucket),
			//~ Key:      store.keyWithPrefix(upload.objectId),
			//~ UploadId: aws.String(upload.multipartId),
		//~ })
	//~ }()

	//~ return nil
//~ }

//~ func (upload *cryptUpload) concatUsingMultipart(ctx context.Context, partialUploads []handler.Upload) error {
	//~ store := upload.store

	//~ numPartialUploads := len(partialUploads)
	//~ errs := make([]error, 0, numPartialUploads)

	//~ // Copy partial uploads concurrently
	//~ var wg sync.WaitGroup
	//~ wg.Add(numPartialUploads)
	//~ for i, partialUpload := range partialUploads {
		//~ // Part numbers must be in the range of 1 to 10000, inclusive. Since
		//~ // slice indexes start at 0, we add 1 to ensure that i >= 1.
		//~ partNumber := int32(i + 1)
		//~ partialS3Upload := partialUpload.(*cryptUpload)

		//~ upload.parts = append(upload.parts, &s3Part{
			//~ number: partNumber,
			//~ size:   -1,
			//~ etag:   "",
		//~ })

		//~ go func(partNumber int32, sourceObject string) {
			//~ defer wg.Done()

			//~ res, err := store.Service.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
				//~ Bucket:     aws.String(store.Bucket),
				//~ Key:        store.keyWithPrefix(upload.objectId),
				//~ UploadId:   aws.String(upload.multipartId),
				//~ PartNumber: aws.Int32(partNumber),
				//~ CopySource: aws.String(store.Bucket + "/" + *store.keyWithPrefix(sourceObject)),
			//~ })
			//~ if err != nil {
				//~ errs = append(errs, err)
				//~ return
			//~ }

			//~ upload.parts[partNumber-1].etag = *res.CopyPartResult.ETag
		//~ }(partNumber, partialS3Upload.objectId)
	//~ }

	//~ wg.Wait()

	//~ if len(errs) > 0 {
		//~ return newMultiError(errs)
	//~ }

	//~ return upload.FinishUpload(ctx)
//~ }

//~ func (upload *cryptUpload) DeclareLength(ctx context.Context, length int64) error {
	//~ info, err := upload.GetInfo(ctx)
	//~ if err != nil {
		//~ return err
	//~ }
	//~ info.Size = length
	//~ info.SizeIsDeferred = false

	//~ return upload.writeInfo(ctx, info)
//~ }

func (store EncryptedStore) listAllParts(ctx context.Context, objectId string, multipartId string) (parts []*s3Part, err error) {
	var partMarker *string
	for {
		t := time.Now()

		// Get uploaded parts
		listPtr, err := store.Service.ListParts(ctx, &s3.ListPartsInput{
			Bucket:           aws.String(store.Bucket),
			Key:              store.keyWithPrefix(objectId),
			UploadId:         aws.String(multipartId),
			PartNumberMarker: partMarker,
		})
		store.observeRequestDuration(t, metricListParts)
		if err != nil {
			return nil, err
		}

		parts = slices.Grow(parts, len(parts)+len((*listPtr).Parts))
		for _, part := range (*listPtr).Parts {
			parts = append(parts, &s3Part{
				number: *part.PartNumber,
				size:   *part.Size,
				etag:   *part.ETag,
			})
		}

		if listPtr.IsTruncated != nil && *listPtr.IsTruncated {
			partMarker = listPtr.NextPartNumberMarker
		} else {
			break
		}
	}
	return parts, nil
}

func (store EncryptedStore) downloadIncompletePartForUpload(ctx context.Context, uploadId string, incompletePartSize int64, iv []byte) (*os.File, error) {
	t := time.Now()
	incompleteUploadObject, err := store.getIncompletePartForUpload(ctx, uploadId, nil)
	if err != nil {
		return nil, err
	}
	if incompleteUploadObject == nil {
		// We did not find an incomplete upload
		return nil, nil
	}
	defer incompleteUploadObject.Body.Close()

	partFile, err := os.CreateTemp(store.TemporaryDirectory, "tusd-s3-tmp-")
	if err != nil {
		return nil, err
	}
    
    // --------------------------------
    decodedPart, err := DecodeObjectAndTrim(incompleteUploadObject.Body, store.block, iv)
    
    decodedReader := bytes.NewReader(decodedPart)
    
    // --------------------------------

	n, err := io.Copy(partFile, decodedReader)
	store.observeRequestDuration(t, metricGetPartObject)
	if err != nil {
		return nil, err
	}
	if n < int64(len(decodedPart)) {
		return nil, errors.New("short read of incomplete upload")
	}

	_, err = partFile.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return partFile, nil
}

func (store EncryptedStore) getIncompletePartForUpload(ctx context.Context, uploadId string, byteRange *string) (*s3.GetObjectOutput, error) {
	obj, err := store.Service.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(store.Bucket),
		Key:    store.metadataKeyWithPrefix(uploadId + ".part"),
        Range: byteRange,
	})

	if err != nil && (isAwsError[*types.NoSuchKey](err) || isAwsError[*types.NotFound](err) || isAwsErrorCode(err, "AccessDenied")) || isAwsErrorCode(err, "Forbidden") {
		return nil, nil
	}

	return obj, err
}

func (store EncryptedStore) headIncompletePartForUpload(ctx context.Context, uploadId string) (int64, error) {
	t := time.Now()
	obj, err := store.Service.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(store.Bucket),
		Key:    store.metadataKeyWithPrefix(uploadId + ".part"),
	})
	store.observeRequestDuration(t, metricHeadPartObject)

	if err != nil {
		if isAwsError[*types.NoSuchKey](err) || isAwsError[*types.NotFound](err) || isAwsErrorCode(err, "AccessDenied") || isAwsErrorCode(err, "Forbidden") {
			err = nil
		}
		return 0, err
	}

	return *obj.ContentLength, nil
}

func (store EncryptedStore) putIncompletePartForUpload(ctx context.Context, uploadId string, file io.ReadSeeker) error {
	t := time.Now()
	_, err := store.Service.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(store.Bucket),
		Key:    store.metadataKeyWithPrefix(uploadId + ".part"),
		Body:   file,
	})
	store.observeRequestDuration(t, metricPutPartObject)
	return err
}

func (store EncryptedStore) deleteIncompletePartForUpload(ctx context.Context, uploadId string) error {
	t := time.Now()
	_, err := store.Service.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(store.Bucket),
		Key:    store.metadataKeyWithPrefix(uploadId + ".part"),
	})
	store.observeRequestDuration(t, metricPutPartObject)
	return err
}

func splitIds(id string) (objectId, multipartId string) {
	index := strings.Index(id, "+")
	if index == -1 {
		return
	}

	objectId = id[:index]
	multipartId = id[index+1:]
	return
}

// isAwsError tests whether an error object is an instance of the AWS error
// specified by its code.
func isAwsError[T error](err error) bool {
	var awsErr T
	return errors.As(err, &awsErr)
}

func isAwsErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == code
	}
	return false
}

func (store EncryptedStore) calcOptimalPartSize(size int64) (optimalPartSize int64, err error) {
	switch {
	// When upload is smaller or equal to PreferredPartSize, we upload in just one part.
	case size <= store.PreferredPartSize:
		optimalPartSize = store.PreferredPartSize
	// Does the upload fit in MaxMultipartParts parts or less with PreferredPartSize.
	case size <= store.PreferredPartSize*store.MaxMultipartParts:
		optimalPartSize = store.PreferredPartSize
	// Prerequisite: Be aware, that the result of an integer division (x/y) is
	// ALWAYS rounded DOWN, as there are no digits behind the comma.
	// In order to find out, whether we have an exact result or a rounded down
	// one, we can check, whether the remainder of that division is 0 (x%y == 0).
	//
	// So if the result of (size/MaxMultipartParts) is not a rounded down value,
	// then we can use it as our optimalPartSize. But if this division produces a
	// remainder, we have to round up the result by adding +1. Otherwise our
	// upload would not fit into MaxMultipartParts number of parts with that
	// size. We would need an additional part in order to upload everything.
	// While in almost all cases, we could skip the check for the remainder and
	// just add +1 to every result, but there is one case, where doing that would
	// doom our upload. When (MaxObjectSize == MaxPartSize * MaxMultipartParts),
	// by adding +1, we would end up with an optimalPartSize > MaxPartSize.
	// With the current S3 API specifications, we will not run into this problem,
	// but these specs are subject to change, and there are other stores as well,
	// which are implementing the S3 API (e.g. RIAK, Ceph RadosGW), but might
	// have different settings.
	case size%store.MaxMultipartParts == 0:
		optimalPartSize = size / store.MaxMultipartParts
	// Having a remainder larger than 0 means, the float result would have
	// digits after the comma (e.g. be something like 10.9). As a result, we can
	// only squeeze our upload into MaxMultipartParts parts, if we rounded UP
	// this division's result. That is what is happending here. We round up by
	// adding +1, if the prior test for (remainder == 0) did not succeed.
	default:
		optimalPartSize = size/store.MaxMultipartParts + 1
	}
    
    // then : ceil the optimal size to a multiple of block size
    //~ remainder := optimalPartSize%store.block.BlockSize()
    //~ if remainder != 0 {
        //~ optimalPartSize += store.block.BlockSize() - remainder
    //~ }
    optimalPartSize = RoundToNextMultiple(optimalPartSize, int64(store.block.BlockSize()))

	// optimalPartSize must never exceed MaxPartSize
	if optimalPartSize > store.MaxPartSize {
		return optimalPartSize, fmt.Errorf("calcOptimalPartSize: to upload %v bytes optimalPartSize %v must exceed MaxPartSize %v", size, optimalPartSize, store.MaxPartSize)
	}
	return optimalPartSize, nil
}

func (store EncryptedStore) keyWithPrefix(key string) *string {
	prefix := store.ObjectPrefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return aws.String(prefix + key)
}

func (store EncryptedStore) metadataKeyWithPrefix(key string) *string {
	prefix := store.MetadataObjectPrefix
	if prefix == "" {
		prefix = store.ObjectPrefix
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return aws.String(prefix + key)
}

func (store EncryptedStore) acquireUploadSemaphore() {
	//~ store.uploadSemaphoreDemandMetric.Inc()
	store.uploadSemaphore.Acquire()
}

func (store EncryptedStore) releaseUploadSemaphore() {
	store.uploadSemaphore.Release()
	//~ store.uploadSemaphoreDemandMetric.Dec()
}

// from tusd internals

func Uid() string {
	id := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, id)
	if err != nil {
		// This is probably an appropriate way to handle errors from our source
		// for random bits.
		panic(err)
	}
	return hex.EncodeToString(id)
}

type Semaphore chan struct{}

// New creates a semaphore with the given concurrency limit.
func NewSemaphore(concurrency int) Semaphore {
	return make(chan struct{}, concurrency)
}

// Acquire will block until the semaphore can be acquired.
func (s Semaphore) Acquire() {
	s <- struct{}{}
}

// Release frees the acquired slot in the semaphore.
func (s Semaphore) Release() {
	<-s
}
