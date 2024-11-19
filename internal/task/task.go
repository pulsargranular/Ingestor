package task

import (
	"context"
	"sync"
)

type TransferMethod int

const (
	TransferS3 TransferMethod = iota + 1
	TransferGlobus
)

type TransferOptions struct {
	S3_endpoint string
	S3_Bucket   string
	Md5checksum bool
}

type TaskTransferConfig struct {
	S3TransferConfig
	GlobusTransferConfig
}

type TaskStatus struct {
	BytesTransferred int
	BytesTotal       int
	FilesTransferred int
	FilesTotal       int
	Failed           bool
	Started          bool
	Finished         bool
	StatusMessage    string
}

type IngestionTask struct {
	// DatasetFolderId   uuid.UUID
	DatasetFolder
	DatasetMetadata map[string]interface{}
	TransferMethod  TransferMethod
	Context         context.Context
	Cancel          context.CancelFunc
	status          *TaskStatus
	statusLock      *sync.RWMutex
}

type Result struct {
	Elapsed_seconds int
	Dataset_PID     string
	Error           error
}

func CreateIngestionTask(datasetFolder DatasetFolder, metadata map[string]interface{}, transferMethod TransferMethod, cancel context.CancelFunc) IngestionTask {
	return IngestionTask{
		DatasetFolder:   datasetFolder,
		DatasetMetadata: metadata,
		TransferMethod:  transferMethod,
		Cancel:          cancel,
		status:          &TaskStatus{},
		statusLock:      &sync.RWMutex{},
	}
}

func (t *IngestionTask) GetStatus() TaskStatus {
	t.statusLock.RLock()
	defer t.statusLock.RUnlock()
	copy := *t.status
	return copy
}

func (t *IngestionTask) SetStatus(
	bytesTransferred *int,
	bytesTotal *int,
	filesTransferred *int,
	filesTotal *int,
	failed *bool,
	started *bool,
	finished *bool,
	statusMessage *string,
) {
	t.statusLock.Lock()
	defer t.statusLock.Unlock()
	if bytesTransferred != nil {
		t.status.BytesTransferred = *bytesTransferred
	}
	if bytesTotal != nil {
		t.status.BytesTotal = *bytesTotal
	}
	if filesTransferred != nil {
		t.status.FilesTransferred = *filesTransferred
	}
	if filesTotal != nil {
		t.status.FilesTotal = *filesTotal
	}
	if failed != nil {
		t.status.Failed = *failed
	}
	if started != nil {
		t.status.Started = *started
	}
	if finished != nil {
		t.status.Finished = *finished
	}
	if statusMessage != nil {
		t.status.StatusMessage = *statusMessage
	}
}
