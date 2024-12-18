package s3upload

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/SwissOpenEM/Ingestor/internal/task"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// Progress notifier object for Minio upload
type TransferNotifier struct {
	totalBytes     int64
	bytesTansfered int64
	FilesCount     int
	startTime      time.Time
	id             uuid.UUID
	notifier       task.ProgressNotifier
	TaskStatus     *task.TaskStatus
}

func (pn *TransferNotifier) AddUploadedBytes(numBytes int64) {
	atomic.AddInt64(&pn.bytesTansfered, numBytes)
}

func (pn *TransferNotifier) UpdateTaskProgress() {
	t := time.Since(pn.startTime)
	pn.notifier.OnTaskProgress(pn.id, float32(pn.bytesTansfered)/float32(pn.totalBytes)*100, int(t.Seconds()))
}

type S3Objects struct {
	Files       []string
	ObjectNames []string
	TotalBytes  int64
}

// Upload all files in a folder using presinged urls
func UploadS3(ctx context.Context, datasetPID string, datasetSourceFolder string, fileList []string, uploadId uuid.UUID, options task.S3TransferConfig, notifier task.ProgressNotifier) error {

	if len(fileList) == 0 {
		return fmt.Errorf("empty file list provided")
	}

	s3Objects := S3Objects{}
	for _, f := range fileList {
		s, _ := os.Stat(path.Join(datasetSourceFolder, f))
		s3Objects.TotalBytes += s.Size()
		s3Objects.Files = append(s3Objects.Files, path.Join(datasetSourceFolder, f))
		s3Objects.ObjectNames = append(s3Objects.ObjectNames, "openem-network/datasets/"+datasetPID+"/raw_files/"+f)
	}

	transferNotifier := TransferNotifier{totalBytes: s3Objects.TotalBytes, bytesTansfered: 0, startTime: time.Now(), id: uploadId, notifier: notifier}

	err := uploadFiles(ctx, &s3Objects, options, &transferNotifier, uploadId)
	return err
}

func uploadFiles(ctx context.Context, s3Objects *S3Objects, options task.S3TransferConfig, transferNotifier *TransferNotifier, uploadId uuid.UUID) error {
	errorGroup, ctx := errgroup.WithContext(ctx)
	objectsChannel := make(chan int, len(s3Objects.Files))

	nWorkers := max(options.ConcurrentFiles, len(s3Objects.Files))

	for t := 0; t < nWorkers; t++ {
		errorGroup.Go(
			func() error {
				for idx := range objectsChannel {
					select {
					case <-ctx.Done():
						transferNotifier.notifier.OnTaskCanceled(uploadId)
						return ctx.Err()
					default:
						err := uploadFile(ctx, s3Objects.Files[idx], s3Objects.ObjectNames[idx], options, transferNotifier)
						if err != nil {
							return err
						}
					}
				}
				return nil
			})
	}
	for idx := range s3Objects.Files {
		objectsChannel <- idx
	}
	close(objectsChannel)
	return errorGroup.Wait()

}
