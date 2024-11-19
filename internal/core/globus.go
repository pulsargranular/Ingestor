package core

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/SwissOpenEM/Ingestor/internal/task"
	"github.com/SwissOpenEM/globus"
	"github.com/google/uuid"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"golang.org/x/oauth2"
)

var globusClient globus.GlobusClient

func GlobusCliLogIn(gConfig *task.GlobusTransferConfig) error {
	// config setup
	ctx := context.Background()
	clientConfig := globus.AuthGenerateOauthClientConfig(ctx, gConfig.ClientID, gConfig.ClientSecret, gConfig.RedirectURL, gConfig.Scopes)
	verifier := oauth2.GenerateVerifier()
	clientConfig.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))

	// redirect user to consent page to ask for permission and obtain the code
	url := clientConfig.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
	fmt.Printf("Visit the URL for the auth dialog: %v\n\nEnter the received code here: ", url)

	// get token
	var code string
	if _, err := fmt.Scan(&code); err != nil {
		return err
	}
	tok, err := clientConfig.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return fmt.Errorf("oauth2 exchange failed: %v", err)
	}

	// setup auto renewal of tokens, update refresh token field in config and create client
	ts := clientConfig.TokenSource(ctx, tok)
	client := oauth2.NewClient(ctx, ts)

	gConfig.RefreshToken = tok.RefreshToken

	// set globus client and return
	GlobusSetHttpClient(client)
	return nil
}

func GlobusLoginWithRefreshToken(gConfig task.GlobusTransferConfig) {
	ctx := context.Background()
	clientConfig := globus.AuthGenerateOauthClientConfig(ctx, gConfig.ClientID, gConfig.ClientSecret, gConfig.RedirectURL, gConfig.Scopes)
	tok := oauth2.Token{
		RefreshToken: gConfig.RefreshToken,
		Expiry:       time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC),
		AccessToken:  "",
		TokenType:    "Bearer",
	}
	ts := clientConfig.TokenSource(ctx, &tok)
	client := oauth2.NewClient(ctx, ts)
	GlobusSetHttpClient(client)
}

func GlobusIsClientReady() bool {
	return globusClient.IsClientSet()
}

/*func GlobusHealthCheck() error {
	// NOTE: this is not a proper health check and takes a long time to finish (~900ms)
	_, err := globusClient.TransferGetTaskList(0, 1)
	return err
}*/

func GlobusSetHttpClient(client *http.Client) {
	globusClient = globus.HttpClientToGlobusClient(client)
}

func globusCheckTransfer(globusTaskId string) (bytesTransferred int, filesTransferred int, totalFiles int, completed bool, err error) {
	globusTask, err := globusClient.TransferGetTaskByID(globusTaskId)
	if err != nil {
		return 0, 0, 1, false, fmt.Errorf("globus: can't continue transfer because an error occured while polling the task \"%s\": %v", globusTaskId, err)
	}
	switch globusTask.Status {
	case "ACTIVE":
		totalFiles := globusTask.Files
		if globusTask.FilesSkipped != nil {
			totalFiles -= *globusTask.FilesSkipped
		}
		return globusTask.BytesTransferred, globusTask.FilesTransferred, totalFiles, false, nil
	case "INACTIVE":
		return 0, 0, 1, false, fmt.Errorf("globus: transfer became inactive, manual intervention required")
	case "SUCCEEDED":
		totalFiles := globusTask.Files
		if globusTask.FilesSkipped != nil {
			totalFiles -= *globusTask.FilesSkipped
		}
		return globusTask.BytesTransferred, globusTask.FilesTransferred, totalFiles, true, nil
	case "FAILED":
		return 0, 0, 1, false, fmt.Errorf("globus: task failed with the following error - code: \"%s\" description: \"%s\"", globusTask.FatalError.Code, globusTask.FatalError.Description)
	default:
		return 0, 0, 1, false, fmt.Errorf("globus: unknown task status: %s", globusTask.Status)
	}
}

func GlobusTransfer(globusConf task.GlobusTransferConfig, task task.IngestionTask, taskCtx context.Context, localTaskId uuid.UUID, datasetFolder string, fileList []datasetIngestor.Datafile, notifier task.ProgressNotifier) error {
	// transfer given filelist
	var filePathList []string
	var fileIsSymlinkList []bool
	for _, file := range fileList {
		filePathList = append(filePathList, filepath.ToSlash(file.Path))
		fileIsSymlinkList = append(fileIsSymlinkList, file.IsSymlink)
	}
	datasetFolder = filepath.ToSlash(datasetFolder)

	s := strings.Split(strings.Trim(datasetFolder, "/"), "/")
	datasetFolderName := s[len(s)-1]

	result, err := globusClient.TransferFileList(
		globusConf.SourceCollection,
		globusConf.SourcePrefixPath+"/"+datasetFolder,
		globusConf.DestinationCollection,
		globusConf.DestinationPrefixPath+"/"+datasetFolderName,
		filePathList,
		fileIsSymlinkList,
		true,
	)
	if err != nil {
		return fmt.Errorf("globus: an error occured when requesting dataset transfer: %v", err)
	}
	if result.Code != "Accepted" {
		return fmt.Errorf("globus: transfer was not accepted - code: \"%s\", message: \"%s\"", result.Code, result.Message)
	}

	// task monitoring
	globusTaskId := result.TaskId
	startTime := time.Now()
	var taskCompleted bool
	var bytesTransferred, filesTransferred, totalFiles int
	falseVal := false

	bytesTransferred, filesTransferred, totalFiles, taskCompleted, err = globusCheckTransfer(globusTaskId)
	if err != nil {
		return err
	}
	if totalFiles == 0 {
		totalFiles = 1 // needed because percentage meter goes NaN otherwise
	}
	task.SetStatus(&bytesTransferred, nil, &filesTransferred, &totalFiles, &falseVal, nil, &taskCompleted, nil)
	if taskCompleted {
		return nil
	}
	notifier.OnTaskProgress(localTaskId, float32(filesTransferred)/float32(totalFiles)*100, int(time.Since(startTime).Seconds()))

	timerUpdater := time.After(1 * time.Second)
	transferUpdater := time.After(1 * time.Minute)
	for {
		select {
		case <-taskCtx.Done():
			// we're cancelling the task
			result, err := globusClient.TransferCancelTaskByID(globusTaskId)
			if err != nil {
				return fmt.Errorf("globus: couldn't cancel task: %v", err)
			}
			if result.Code != "Canceled" {
				return fmt.Errorf("globus: couldn't cancel task - code: \"%s\", message: \"%s\"", result.Code, result.Message)
			}
			status := "task was cancelled"
			task.SetStatus(nil, nil, nil, nil, nil, nil, nil, &status)
			notifier.OnTaskCanceled(localTaskId)
			return nil
		case <-timerUpdater:
			// update timer every second
			timerUpdater = time.After(1 * time.Second)
			notifier.OnTaskProgress(localTaskId, float32(filesTransferred)/float32(totalFiles)*100, int(time.Since(startTime).Seconds()))
		case <-transferUpdater:
			// check state of transfer
			transferUpdater = time.After(1 * time.Minute)
			bytesTransferred, filesTransferred, totalFiles, taskCompleted, err = globusCheckTransfer(globusTaskId)
			if err != nil {
				return err // transfer cannot be finished: irrecoverable error
			}
			if totalFiles == 0 {
				totalFiles = 1 // needed because percentage meter goes NaN otherwise
			}

			task.SetStatus(&bytesTransferred, nil, &filesTransferred, &totalFiles, nil, nil, nil, nil)
			notifier.OnTaskProgress(localTaskId, float32(filesTransferred)/float32(totalFiles)*100, int(time.Since(startTime).Seconds()))

			if taskCompleted {
				return nil // we're done!
			}
		}
	}
}
