package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sync"

	"github.com/alitto/pond/v2"
	"github.com/minio/minio-go/v7"
)

const (
	chunkSize            = 5 * 1024 * 1024 // 5 MB
	server               = "http://localhost:8888"
	presigned_url_path   = "/presignedUrls"
	complete_upload_path = "/completeUpload"
)

type presignedUrlBody struct {
	ObjectName string `json:"object_name"`
	Parts      int    `json:"parts"`
}

type presignedUrlResp struct {
	UploadID string   `json:"uploadID"`
	Urls     []string `json:"urls"`
}

type completeUploadBody struct {
	ObjectName string               `json:"object_name"`
	UploadID   string               `json:"uploadID"`
	Parts      []minio.CompletePart `json:"parts"`
}

type MultipartInput struct {
	File      *os.File
	PartCount int
}

type HttpUploader struct {
	Pool pond.Pool
}

var instance *HttpUploader
var once sync.Once

func GetHttpUploader() *HttpUploader {
	once.Do(func() {
		instance = &HttpUploader{Pool: pond.NewPool(100)}
	})
	return instance
}

// Fetches presigned url(s) from API server. If parts > 1, multipart upload
// is initiated
func getPresignedUrls(object_name string, parts int, endpoint string) (string, []string, error) {
	body := presignedUrlBody{
		ObjectName: object_name,
		Parts:      parts,
	}
	jsonBody, _ := json.Marshal(body)
	bodyReader := bytes.NewReader(jsonBody)

	req, err := http.NewRequest("POST", endpoint+presigned_url_path, bodyReader)

	if err != nil {
		return "", []string{}, fmt.Errorf("error creating request for %s. error: %s", object_name, err.Error())
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return "", []string{}, fmt.Errorf("error executing request for %s. error: %s", object_name, err.Error())
	}

	resBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", []string{}, fmt.Errorf("failed to read response body for %s. error: %s", object_name, err.Error())
	}

	var result presignedUrlResp
	if err := json.Unmarshal(resBody, &result); err != nil {
		return "", []string{}, fmt.Errorf("error unmarshalling JSON for %s. error: %s", object_name, err.Error())
	}

	return result.UploadID, result.Urls, nil

}

func completeMultiPartUpload(object_name string, uploadID string, parts []minio.CompletePart) error {
	body := completeUploadBody{
		ObjectName: object_name,
		UploadID:   uploadID,
		Parts:      parts,
	}
	jsonBody, _ := json.Marshal(body)
	fmt.Println(string(jsonBody))
	bodyReader := bytes.NewReader(jsonBody)
	req, _ := http.NewRequest("POST", server+complete_upload_path, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// return errors.New("Fail")
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

	err = doUploadMultipart(ctx, totalSize, objectName, file, endpoint, notifier)
	return err

}

func doUploadSingleFile(ctx context.Context, objectName string, file *os.File, endpoint string, notifier *TransferNotifier) error {

	_, url, err := getPresignedUrls(objectName, 1, endpoint)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return err
	}

	base64hash := calculateHashB64(&data)
	_, err = uploadData(ctx, &data, url[0], base64hash)
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

func doUploadMultipart(ctx context.Context, totalSize int64, objectName string, file *os.File, endpoint string, notifier *TransferNotifier) error {
	partCount := int(math.Ceil(float64(totalSize) / float64(chunkSize)))

	uploadID, presignedURLs, err := getPresignedUrls(objectName, partCount, endpoint)
	if err != nil {
		return err
	}

	uploader := GetHttpUploader()

	group := uploader.Pool.NewGroupContext(ctx)
	parts := make([]minio.CompletePart, partCount)

	for partNumber := 0; partNumber < partCount; partNumber++ {
		group.SubmitErr(func() error {
			partData := make([]byte, chunkSize)
			n, _ := file.ReadAt(partData, int64(partNumber)*chunkSize)
			partData = partData[:n]

			base64hash := calculateHashB64(&partData)
			resp, err := uploadData(ctx, &partData, presignedURLs[partNumber], base64hash)
			if err != nil {
				return err
			}

			notifier.AddUploadedBytes(int64(n))
			if partNumber%2 == 0 {
				notifier.UpdateTaskProgress()
			}
			parts[partNumber] = minio.CompletePart{ETag: resp.Header.Get("ETag"), PartNumber: partNumber + 1, ChecksumSHA256: base64hash}

			fmt.Printf("Uploaded part %d\n", partNumber+1)
			return nil
		})
	}

	group.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	err = completeMultiPartUpload(objectName, uploadID, parts)
	if err != nil {
		return fmt.Errorf("error completing multipart upload: %w", err)
	}

	fmt.Println("Multipart upload completed successfully.")
	return nil
}

func calculateHashB64(data *[]byte) string {
	hash := sha256.Sum256(*data)
	base64hash := base64.StdEncoding.EncodeToString(hash[:])
	return base64hash
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return resp, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("upload failed: %d %s", resp.StatusCode, resp.Status)
	}
	return resp, nil
}
