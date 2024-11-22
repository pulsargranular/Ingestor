package s3upload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/alitto/pond/v2"
)

const (
	chunkSize = 5 * 1024 * 1024 // 5 MB
)

type MultipartInput struct {
	File      *os.File
	PartCount int
}

type HttpUploader struct {
	Pool   pond.Pool
	Client http.Client
}

var instance *HttpUploader
var once sync.Once

func GetHttpUploader() *HttpUploader {
	once.Do(func() {
		instance = &HttpUploader{Pool: pond.NewPool(runtime.NumCPU()), Client: http.Client{}}
	})
	return instance
}

var presignedUrlServerClient *ClientWithResponses
var once_presignedUrlServerClient sync.Once

func GetPresignedUrlServer() *ClientWithResponses {
	once_presignedUrlServerClient.Do(func() {
		presignedUrlServerClient, _ = NewClientWithResponses("http://localhost:8888")
	})
	return presignedUrlServerClient
}

// Fetches presigned url(s) from API server. If parts > 1, multipart upload
// is initiated
func getPresignedUrls(object_name string, part int, endpoint string) (string, []string, error) {

	r, err := GetPresignedUrlServer().GetPresignedUrlsWithResponse(context.Background(), PresignedUrlBody{
		ObjectName: object_name,
		Parts:      part,
	})

	if err != nil {
		return "", []string{}, err
	}
	if r.StatusCode() != http.StatusOK {
		return "", []string{}, fmt.Errorf("")
	}

	return r.JSON200.UploadID, r.JSON200.Urls, err
}

func completeMultiPartUpload(object_name string, uploadID string, endpoint string, parts []CompletePart, full_file_checksum string) error {
	r, err := GetPresignedUrlServer().CompleteUploadWithResponse(context.Background(), CompleteUploadBody{
		ObjectName:     object_name,
		UploadID:       uploadID,
		Parts:          parts,
		ChecksumSHA256: full_file_checksum,
	})

	if err != nil {
		return err
	}
	if r.StatusCode() != http.StatusOK {
		return fmt.Errorf("")
	}
	return nil
}

func uploadFile(ctx context.Context, filePath string, objectName string, endpoint string, notifier *TransferNotifier) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}

	defer file.Close()

	// Get the file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("error getting file info: %w", err)
	}

	totalSize := fileInfo.Size()
	fmt.Printf("Uploading file: %s (%d bytes)\n", filePath, totalSize)

	if totalSize < chunkSize {
		// do normal upload
		err := doUploadSingleFile(ctx, objectName, file, endpoint, notifier)
		return err

	}

	uploadID, err := doUploadMultipart(ctx, totalSize, objectName, file, endpoint, notifier)
	if err != nil {
		err = abortMultipartUpload(uploadID, objectName, endpoint)
		if err != nil {
			slog.Error("Failed to abort mulitpart upload", "uploadID", uploadID, "object", objectName)
		}
	}
	return err

}

func abortMultipartUpload(uploadID string, objectName string, endpoint string) error {
	r, err := GetPresignedUrlServer().AbortMultipartUploadWithResponse(context.Background(), AbortUploadBody{
		ObjectName: objectName,
		UploadID:   uploadID,
	})

	if err != nil {
		return err
	}
	if r.StatusCode() != http.StatusOK {
		return fmt.Errorf("")
	}
	return nil

}

func doUploadSingleFile(ctx context.Context, objectName string, file *os.File, endpoint string, notifier *TransferNotifier) error {

	_, urls, err := getPresignedUrls(objectName, 1, endpoint)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return err
	}

	base64hash, _ := calculateHashB64(&data)
	_, err = uploadData(ctx, &data, urls[0], base64hash)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	notifier.AddUploadedBytes(int64(len(data)))
	notifier.UpdateTaskProgress()
	return nil
}

func doUploadMultipart(ctx context.Context, totalSize int64, objectName string, file *os.File, endpoint string, notifier *TransferNotifier) (string, error) {
	partCount := int(math.Ceil(float64(totalSize) / float64(chunkSize)))

	uploadID, presignedURLs, err := getPresignedUrls(objectName, partCount, endpoint)
	if err != nil {
		return uploadID, err
	}

	uploader := GetHttpUploader()

	group := uploader.Pool.NewGroupContext(ctx)
	parts := make([]CompletePart, partCount)
	partChecksums := make([]string, partCount)

	for partNumber := 0; partNumber < partCount; partNumber++ {
		group.SubmitErr(func() error {
			partData := make([]byte, chunkSize)
			n, _ := file.ReadAt(partData, int64(partNumber)*chunkSize)
			partData = partData[:n]

			base64hash, hash := calculateHashB64(&partData)
			partChecksums[partNumber] = string(hash[:])
			resp, err := uploadData(ctx, &partData, presignedURLs[partNumber], base64hash)
			if err != nil {
				return err
			}

			notifier.AddUploadedBytes(int64(n))

			notifier.UpdateTaskProgress()

			etag := strings.Replace(resp.Header.Get("ETag"), "\"", "", -1)
			parts[partNumber] = CompletePart{ETag: etag, PartNumber: partNumber + 1, ChecksumSHA256: base64hash}

			fmt.Printf("Uploaded part %d\n", partNumber+1)
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
	slog.Info("Calculated file digest", "file", file.Name(), "sha256", base64hash)

	err = completeMultiPartUpload(objectName, uploadID, endpoint, parts, base64hash)
	if err != nil {
		return uploadID, fmt.Errorf("error completing multipart upload: %w", err)
	}

	fmt.Println("Multipart upload completed successfully.")
	return uploadID, nil
}

func calculateHashB64(data *[]byte) (string, [32]byte) {
	hash := sha256.Sum256(*data)
	base64hash := base64.StdEncoding.EncodeToString(hash[:])
	return base64hash, hash
}

func uploadData(ctx context.Context, data *[]byte, presignedURL string, base64hash string) (*http.Response, error) {

	decoded_url, _ := base64.StdEncoding.DecodeString(presignedURL)
	req, err := http.NewRequestWithContext(ctx, "PUT", string(decoded_url), bytes.NewReader(*data))

	if err != nil {
		return nil, err
	}

	// The checksum algorithm needs to match the one defined in the presigned url
	req.Header.Set("x-amz-checksum-sha256", base64hash)
	req.Header.Set("Content-Type", "application/json")

	resp, err := GetHttpUploader().Client.Do(req)
	if err != nil {
		return resp, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("upload failed: %d %s", resp.StatusCode, resp.Status)
	}
	return resp, nil
}

// func doRequest(method string, jsonBody []byte, path string, endpoint string) ([]byte, error) {
// 	bodyReader := bytes.NewReader(jsonBody)
// 	req, err := http.NewRequest(method, endpoint+path, bodyReader)

// 	if err != nil {
// 		return []byte{}, err
// 	}

// 	req.Header.Set("Content-Type", "application/json")
// 	resp, err := GetHttpUploader().Client.Do(req)
// 	if err != nil {
// 		return []byte{}, err
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode != http.StatusOK {
// 		return []byte{}, fmt.Errorf("%s request failed: %s%s", method, endpoint, path)
// 	}

// 	resBody, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		return []byte{}, err
// 	}
// 	return resBody, nil
// }
