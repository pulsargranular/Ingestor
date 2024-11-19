package core

import (
	"context"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SwissOpenEM/Ingestor/internal/task"
	"github.com/google/uuid"
)

// Progress notifier object for Minio upload
type TransferNotifier struct {
	totalBytes     int64
	bytesTansfered int64
	FilesCount     int
	startTime      time.Time
	id             uuid.UUID
	notifier       ProgressNotifier
	TaskStatus     *task.TaskStatus
}

func (pn *TransferNotifier) AddUploadedBytes(numBytes int64) {
	atomic.AddInt64(&pn.bytesTansfered, numBytes)
}

func (pn *TransferNotifier) UpdateTaskProgress() {
	t := time.Since(pn.startTime)
	pn.notifier.OnTaskProgress(pn.id, float32(pn.bytesTansfered)/float32(pn.totalBytes)*100, int(t.Seconds()))
}

// Upload all files in a folder using presinged urls
func UploadS3(ctx context.Context, datasetPID string, datasetSourceFolder string, fileList []string, uploadId uuid.UUID, options task.S3TransferConfig, notifier ProgressNotifier) error {

	if len(fileList) == 0 {
		return nil
	}

	totalBytes := int64(0)
	for _, f := range fileList {
		s, _ := os.Stat(path.Join(datasetSourceFolder, f))
		totalBytes += s.Size()
	}

	transferNotifier := TransferNotifier{totalBytes: totalBytes, bytesTansfered: 0, startTime: time.Now(), id: uploadId, notifier: notifier}

	wg := sync.WaitGroup{}
	filesChannel := make(chan string, len(fileList))
	nWorkers := max(1, len(fileList))
	// start the workers
	for t := 0; t < nWorkers; t++ {
		wg.Add(1)
		go func(filesChannel <-chan string, wg *sync.WaitGroup) {
			for f := range filesChannel {
				select {
				case <-ctx.Done():
					transferNotifier.notifier.OnTaskCanceled(uploadId)
					wg.Done()
					return
				default:
					filePath := path.Join(datasetSourceFolder, f)
					objectName := "openem-network/datasets/" + datasetPID + "/raw_files/" + f
					uploadFile(ctx, filePath, objectName, options.Endpoint, &transferNotifier)
				}
			}
			wg.Done()
		}(filesChannel, &wg)
	}
	for _, f := range fileList {
		filesChannel <- f
	}
	close(filesChannel)
	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return nil
}
