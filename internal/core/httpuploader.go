package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/alitto/pond/v2"
	"github.com/minio/minio-go/v7"
)

const (
	chunkSize                = 5 * 1024 * 1024 // 5 MB
	presignedUrlPath         = "/presignedUrls"
	completeUploadPath       = "/completeUpload"
	abortMultiPartUplaodPath = "/abortMultipartUpload"
)

type presignedUrlBody struct {
	ObjectName string `json:"object_name"`
	Parts      int    `json:"parts"`
}

type presignedUrlRespMultipart struct {
	UploadID string   `json:"uploadID"`
	Urls     []string `json:"urls"`
}

type presignedUrlResp struct {
	Url string `json:"url"`
}

type completeMultipartUploadBody struct {
	ObjectName     string               `json:"object_name"`
	UploadID       string               `json:"uploadID"`
	Parts          []minio.CompletePart `json:"parts"`
	ChecksumSHA256 string               `json:"checksumSHA256"`
}
type abortMultipartUploadBody struct {
	ObjectName string `json:"object_name"`
	UploadID   string `json:"uploadID"`
}

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
		instance = &HttpUploader{Pool: pond.NewPool(100)}
	})
	return instance
}

// Fetches presigned url(s) from API server. If parts > 1, multipart upload
// is initiated
func getPresignedUrlsMultipart(object_name string, part int, endpoint string) (string, []string, error) {
	body := presignedUrlBody{
		ObjectName: object_name,
		Parts:      part,
	}
	jsonBody, _ := json.Marshal(body)
	resBody, err := doRequest("POST", jsonBody, presignedUrlPath, endpoint)

	if err != nil {
		return "", []string{}, err
	}

	var result presignedUrlRespMultipart
	if err := json.Unmarshal(resBody, &result); err != nil {
		return "", []string{}, fmt.Errorf("error unmarshalling JSON for %s. error: %s", object_name, err.Error())
	}

	return result.UploadID, result.Urls, nil
}

// Fetches presigned url(s) from API server. If parts > 1, multipart upload
// is initiated
func getPresignedUrl(object_name string, endpoint string) (string, error) {
	body := presignedUrlBody{
		ObjectName: object_name,
		Parts:      1,
	}
	jsonBody, _ := json.Marshal(body)
	resBody, err := doRequest("POST", jsonBody, presignedUrlPath, endpoint)

	if err != nil {
		return "", err
	}

	var result presignedUrlResp
	if err := json.Unmarshal(resBody, &result); err != nil {
		return "", fmt.Errorf("error unmarshalling JSON for %s. error: %s", object_name, err.Error())
	}

	return result.Url, err

}

func completeMultiPartUpload(object_name string, uploadID string, endpoint string, parts []minio.CompletePart, full_file_checksum string) error {
	body := completeMultipartUploadBody{
		ObjectName:     object_name,
		UploadID:       uploadID,
		Parts:          parts,
		ChecksumSHA256: full_file_checksum,
	}
	jsonBody, _ := json.Marshal(body)
	fmt.Println(string(jsonBody))
	bodyReader := bytes.NewReader(jsonBody)
	req, _ := http.NewRequest("POST", endpoint+completeUploadPath, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	resp, err := GetHttpUploader().Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("Fail")
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
		abortMultipartUpload(uploadID, objectName, endpoint)
		if err != nil {
			slog.Error("Failed to abort mulitpart upload", "uploadID", uploadID, "object", objectName)
		}
	}
	return err

}

func abortMultipartUpload(uploadID string, objectName string, endpoint string) error {
	body := abortMultipartUploadBody{
		ObjectName: objectName,
		UploadID:   uploadID,
	}

	jsonBody, _ := json.Marshal(body)
	_, err := doRequest("POST", jsonBody, abortMultiPartUplaodPath, endpoint)
	return err
}

func doUploadSingleFile(ctx context.Context, objectName string, file *os.File, endpoint string, notifier *TransferNotifier) error {

	url, err := getPresignedUrl(objectName, endpoint)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return err
	}

	base64hash, _ := calculateHashB64(&data)
	_, err = uploadData(ctx, &data, url, base64hash)
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

	uploadID, presignedURLs, err := getPresignedUrlsMultipart(objectName, partCount, endpoint)
	if err != nil {
		return uploadID, err
	}

	uploader := GetHttpUploader()

	group := uploader.Pool.NewGroupContext(ctx)
	parts := make([]minio.CompletePart, partCount)
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
			parts[partNumber] = minio.CompletePart{ETag: etag, PartNumber: partNumber + 1, ChecksumSHA256: base64hash}

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

func doRequest(method string, jsonBody []byte, path string, endpoint string) ([]byte, error) {
	bodyReader := bytes.NewReader(jsonBody)
	req, err := http.NewRequest(method, endpoint+presignedUrlPath, bodyReader)

	if err != nil {
		return []byte{}, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := GetHttpUploader().Client.Do(req)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return []byte{}, fmt.Errorf("%s request failed: %s%s", method, endpoint, path)
	}

	resBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}
	return resBody, nil
}
