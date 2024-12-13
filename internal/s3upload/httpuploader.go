package s3upload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/SwissOpenEM/Ingestor/internal/task"
	"github.com/alitto/pond/v2"
	"github.com/hashicorp/go-retryablehttp"
)

const (
	MiB = 1024 * 1024
)

type MultipartInput struct {
	File      *os.File
	PartCount int
}

type HttpUploader struct {
	Pool   pond.Pool
	Client *http.Client
}

var instance *HttpUploader
var once sync.Once

func GetHttpUploader(poolSize int) *HttpUploader {
	once.Do(func() {
		retryClient := retryablehttp.NewClient()
		retryClient.RetryMax = 10
		retryClient.Backoff = retryablehttp.DefaultBackoff

		standardClient := retryClient.StandardClient()
		instance = &HttpUploader{Pool: pond.NewPool(poolSize), Client: standardClient}
	})
	return instance
}

var presignedUrlServerClient *ClientWithResponses
var once_presignedUrlServerClient sync.Once

func GetPresignedUrlServer(endpoint string) *ClientWithResponses {
	once_presignedUrlServerClient.Do(func() {
		presignedUrlServerClient, _ = NewClientWithResponses(endpoint)
	})
	return presignedUrlServerClient
}

// Fetches presigned url(s) from API server. If parts > 1, multipart upload
// is initiated
func getPresignedUrls(object_name string, part int, endpoint string) (string, []string, error) {

	response, err := GetPresignedUrlServer(endpoint).GetPresignedUrlsWithResponse(context.Background(), PresignedUrlBody{
		ObjectName: object_name,
		Parts:      part,
	})

	if err != nil {
		return "", []string{}, err
	}

	if response.StatusCode() == http.StatusInternalServerError {
		return "", []string{}, fmt.Errorf("%s: %s", response.JSON500.Message, *response.JSON500.Details)
	}
	if response.StatusCode() == http.StatusUnprocessableEntity {
		err_string := ""
		for _, d := range *response.JSON422.Detail {
			err_string += " " + d.Msg
		}

		return "", []string{}, fmt.Errorf("%s", err_string)
	}
	return response.JSON200.UploadID, response.JSON200.Urls, err
}

func completeMultiPartUpload(object_name string, uploadID string, endpoint string, parts []CompletePart, full_file_checksum string) error {
	response, err := GetPresignedUrlServer(endpoint).CompleteUploadWithResponse(context.Background(), CompleteUploadBody{
		ObjectName:     object_name,
		UploadID:       uploadID,
		Parts:          parts,
		ChecksumSHA256: full_file_checksum,
	})

	if err != nil {
		return err
	}

	if response.StatusCode() == http.StatusInternalServerError {
		return fmt.Errorf("%s: %s", response.JSON500.Message, *response.JSON500.Details)
	}
	if response.StatusCode() == http.StatusUnprocessableEntity {
		err_string := ""
		for _, d := range *response.JSON422.Detail {
			err_string += " " + d.Msg
		}

		return fmt.Errorf("%s", err_string)
	}
	return nil
}

func uploadFile(ctx context.Context, filePath string, objectName string, options task.S3TransferConfig, notifier *TransferNotifier) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}

	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("error getting file info: %w", err)
	}

	totalSize := fileInfo.Size()
	httpClient := GetHttpUploader(options.PoolSize)

	if totalSize < options.ChunkSizeMB*MiB {
		err := doUploadSingleFile(ctx, objectName, file, httpClient, options.Endpoint, notifier)
		return err

	}

	uploadID, err := doUploadMultipart(ctx, totalSize, objectName, file, httpClient, options, notifier)
	if err != nil {
		err_upload := fmt.Errorf("failed to do multipart upload: uploadID=%s,  objectName=%s, error=%s", uploadID, objectName, err.Error())
		err_abort := abortMultipartUpload(uploadID, objectName, options.Endpoint)
		if err_abort != nil {
			return fmt.Errorf("while aborting a multipart upload an error occured: %s. Previous error: %s", err_abort.Error(), err_upload.Error())
		}
		return err_upload
	}
	return err
}

func abortMultipartUpload(uploadID string, objectName string, endpoint string) error {
	response, err := GetPresignedUrlServer(endpoint).AbortMultipartUploadWithResponse(context.Background(), AbortUploadBody{
		ObjectName: objectName,
		UploadID:   uploadID,
	})

	if err != nil {
		return err
	}
	if response.StatusCode() != http.StatusOK {
		return fmt.Errorf("")
	}
	return nil
}

func doUploadSingleFile(ctx context.Context, objectName string, file *os.File, httpClient *HttpUploader, endpoint string, notifier *TransferNotifier) error {

	_, urls, err := getPresignedUrls(objectName, 1, endpoint)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	n := len(data)

	base64hash, _ := calculateHashB64(&data)
	_, err = uploadData(ctx, &data, urls[0], httpClient, base64hash)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	notifier.AddUploadedBytes(int64(n))
	notifier.UpdateTaskProgress()
	return nil
}

func doUploadMultipart(ctx context.Context, totalSize int64, objectName string, file *os.File, httpClient *HttpUploader, options task.S3TransferConfig, notifier *TransferNotifier) (string, error) {
	partCount := int(math.Ceil(float64(totalSize) / float64(options.ChunkSizeMB*MiB)))

	uploadID, presignedURLs, err := getPresignedUrls(objectName, partCount, options.Endpoint)
	if err != nil {
		return uploadID, err
	}

	group := httpClient.Pool.NewGroupContext(ctx)
	parts := make([]CompletePart, partCount)
	partChecksums := make([]string, partCount)

	for partNumber := 0; partNumber < partCount; partNumber++ {
		group.SubmitErr(func() error {
			partData := make([]byte, options.ChunkSizeMB*MiB)
			n, _ := file.ReadAt(partData, int64(partNumber)*options.ChunkSizeMB*MiB)
			partData = partData[:n]

			base64hash, hash := calculateHashB64(&partData)
			partChecksums[partNumber] = string(hash[:])
			etag, err := uploadData(ctx, &partData, presignedURLs[partNumber], httpClient, base64hash)
			if err != nil {
				return err
			}

			notifier.AddUploadedBytes(int64(n))

			notifier.UpdateTaskProgress()

			parts[partNumber] = CompletePart{ETag: etag, PartNumber: partNumber + 1, ChecksumSHA256: base64hash}

			return nil
		})
	}

	err = group.Wait()

	if err != nil {
		return uploadID, ctx.Err()
	}

	c := strings.Join(partChecksums, "")
	n := sha256.Sum256([]byte(c))
	base64hash := base64.StdEncoding.EncodeToString(n[:])

	err = completeMultiPartUpload(objectName, uploadID, options.Endpoint, parts, base64hash)
	if err != nil {
		return uploadID, fmt.Errorf("error completing multipart upload: %s", err.Error())
	}

	return uploadID, nil
}

func calculateHashB64(data *[]byte) (string, [32]byte) {
	hash := sha256.Sum256(*data)
	base64hash := base64.StdEncoding.EncodeToString(hash[:])
	return base64hash, hash
}

func uploadData(ctx context.Context, data *[]byte, presignedURL string, httpClient *HttpUploader, base64hash string) (string, error) {

	decoded_url, _ := base64.StdEncoding.DecodeString(presignedURL)
	req, err := http.NewRequestWithContext(ctx, "PUT", string(decoded_url), bytes.NewReader(*data))

	if err != nil {
		return "", err
	}

	// The checksum algorithm needs to match the one defined in the presigned url
	req.Header.Set("x-amz-checksum-sha256", base64hash)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Client.Do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	etag := strings.Replace(resp.Header.Get("ETag"), "\"", "", -1)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("upload failed: %d %s", resp.StatusCode, resp.Status)
	}
	return etag, nil
}
