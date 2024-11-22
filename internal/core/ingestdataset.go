package core

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SwissOpenEM/Ingestor/internal/metadataextractor"
	"github.com/SwissOpenEM/Ingestor/internal/s3upload"
	"github.com/SwissOpenEM/Ingestor/internal/task"
	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

const MAX_FILES = 400000

func createLocalSymlinkCallbackForFileLister(skipSymlinks *string, skippedLinks *uint) func(symlinkPath string, sourceFolder string) (bool, error) {
	scanner := bufio.NewScanner(os.Stdin)
	return func(symlinkPath string, sourceFolder string) (bool, error) {
		var keep bool
		pointee, _ := os.Readlink(symlinkPath) // just pass the file name
		if !filepath.IsAbs(pointee) {
			symlinkAbs, err := filepath.Abs(filepath.Dir(symlinkPath))
			if err != nil {
				return false, err
			}
			// log.Printf(" CWD path pointee :%v %v %v", dir, filepath.Dir(path), pointee)
			pointeeAbs := filepath.Join(symlinkAbs, pointee)
			pointee, err = filepath.EvalSymlinks(pointeeAbs)
			if err != nil {
				log.Printf("Could not follow symlink for file:%v %v", pointeeAbs, err)
				keep = false
				log.Printf("keep variable set to %v", keep)
			}
		}
		//fmt.Printf("Skip variable:%v\n", *skip)
		if *skipSymlinks == "ka" || *skipSymlinks == "kA" {
			keep = true
		} else if *skipSymlinks == "sa" || *skipSymlinks == "sA" {
			keep = false
		} else if *skipSymlinks == "da" || *skipSymlinks == "dA" {
			keep = strings.HasPrefix(pointee, sourceFolder)
		} else {
			color.Set(color.FgYellow)
			log.Printf("Warning: the file %s is a link pointing to %v.", symlinkPath, pointee)
			color.Unset()
			log.Printf(`
	Please test if this link is meaningful and not pointing 
	outside the sourceFolder %s. The default behaviour is to
	keep only internal links within a source folder.
	You can also specify that you want to apply the same answer to ALL 
	subsequent links within the current dataset, by appending an a (dA,ka,sa).
	If you want to give the same answer even to all subsequent datasets 
	in this command then specify a capital 'A', e.g. (dA,kA,sA)
	Do you want to keep the link in dataset or skip it (D(efault)/k(eep)/s(kip) ?`, sourceFolder)
			scanner.Scan()
			*skipSymlinks = scanner.Text()
			if *skipSymlinks == "" {
				*skipSymlinks = "d"
			}
			if *skipSymlinks == "d" || *skipSymlinks == "dA" {
				keep = strings.HasPrefix(pointee, sourceFolder)
			} else {
				keep = (*skipSymlinks != "s" && *skipSymlinks != "sa" && *skipSymlinks != "sA")
			}
		}
		if keep {
			color.Set(color.FgGreen)
			log.Printf("You chose to keep the link %v -> %v.\n\n", symlinkPath, pointee)
		} else {
			color.Set(color.FgRed)
			*skippedLinks++
			log.Printf("You chose to remove the link %v -> %v.\n\n", symlinkPath, pointee)
		}
		color.Unset()
		return keep, nil
	}
}

func createLocalFilenameFilterCallback(illegalFileNamesCounter *uint) func(filepath string) bool {
	return func(filepath string) (keep bool) {
		keep = true
		// make sure that filenames do not contain characters like "\" or "*"
		if strings.ContainsAny(filepath, "*\\") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains illegal characters like *,\\ and will not be archived.", filepath)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			keep = false
		}
		// and check for triple blanks, they are used to separate columns in messages
		if keep && strings.Contains(filepath, "   ") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains 3 consecutive blanks which is not allowed. The file not be archived.", filepath)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			keep = false
		}
		return keep
	}
}

func IngestDataset(
	task_context context.Context,
	ingestionTask task.IngestionTask,
	config Config,
	notifier task.ProgressNotifier,
) (string, error) {
	var http_client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		Timeout:   120 * time.Second}

	SCICAT_API_URL := config.Scicat.Host

	const TAPECOPIES = 2 // dummy value, unused
	const DATASETFILELISTTXT = ""

	var skipSymlinks string = "dA" // skip all simlinks

	user := map[string]string{
		"accessToken": config.Scicat.AccessToken,
	}

	datasetFolder := ingestionTask.DatasetFolder.FolderPath

	var metaDataMap map[string]interface{}
	if len(ingestionTask.DatasetMetadata) > 0 {
		metaDataMap = ingestionTask.DatasetMetadata
	} else {
		var err error
		metadatafile := metadataextractor.MetadataFilePath(datasetFolder)
		if _, err = os.Stat(metadatafile); errors.Is(err, os.ErrNotExist) {
			return "", err
		}

		metaDataMap, err = datasetIngestor.ReadMetadataFromFile(metadatafile)
		if err != nil {
			return "", err
		}
	}

	fullUser, accessGroups, err := datasetUtils.GetUserInfoFromToken(http_client, SCICAT_API_URL, config.Scicat.AccessToken)
	if err != nil {
		return "", err
	}

	// check if dataset already exists (identified by source folder)
	_, _, err = datasetIngestor.CheckMetadata(http_client, SCICAT_API_URL, metaDataMap, fullUser, accessGroups)
	if err != nil {
		return "", err
	}

	var skippedLinks uint = 0
	var illegalFileNames uint = 0
	localSymlinkCallback := createLocalSymlinkCallbackForFileLister(&skipSymlinks, &skippedLinks)
	localFilepathFilterCallback := createLocalFilenameFilterCallback(&illegalFileNames)

	// collect (local) files
	fullFileArray, startTime, endTime, owner, numFiles, totalSize, err := datasetIngestor.GetLocalFileList(datasetFolder, DATASETFILELISTTXT, localSymlinkCallback, localFilepathFilterCallback)
	if err != nil {
		log.Printf("")
		return "", err
	}

	// size & filecount checks
	if totalSize == 0 {
		return "", errors.New("can't ingest: the total size of the dataset is 0")
	}
	if numFiles > MAX_FILES {
		return "", fmt.Errorf("can't ingest: the number of files (%d) exceeds the max. allowed (%d)", numFiles, MAX_FILES)
	}

	originalMetaDataMap := map[string]string{}
	datasetIngestor.UpdateMetaData(http_client, SCICAT_API_URL, user, originalMetaDataMap, metaDataMap, startTime, endTime, owner, TAPECOPIES)

	intTotalSize := int(totalSize)
	totalFiles := len(fullFileArray)
	trueVal := true
	ingestionTask.SetStatus(nil, &intTotalSize, nil, &totalFiles, nil, &trueVal, nil, nil)

	metaDataMap["datasetlifecycle"] = map[string]interface{}{}
	metaDataMap["datasetlifecycle"].(map[string]interface{})["isOnCentralDisk"] = false
	metaDataMap["datasetlifecycle"].(map[string]interface{})["archiveStatusMessage"] = "filesNotYetAvailable"
	metaDataMap["datasetlifecycle"].(map[string]interface{})["archivable"] = false

	datasetId, err := datasetIngestor.IngestDataset(http_client, SCICAT_API_URL, metaDataMap, fullFileArray, user)

	// TODO: add attachments here if it's going to be needed

	switch ingestionTask.TransferMethod {
	case task.TransferS3:
		fileList := []string{}
		for _, f := range fullFileArray {
			fileList = append(fileList, f.Path)
		}
		err = s3upload.UploadS3(task_context, datasetId, datasetFolder, fileList, ingestionTask.DatasetFolder.Id, config.Transfer.S3, notifier)
	case task.TransferGlobus:
		// globus doesn't work with absolute folders, this library uses sourcePrefix to adapt the path to the globus' own path from a relative path
		relativeDatasetFolder := strings.TrimPrefix(datasetFolder, config.WebServer.CollectionLocation)
		err = GlobusTransfer(config.Transfer.Globus, ingestionTask, task_context, ingestionTask.DatasetFolder.Id, relativeDatasetFolder, fullFileArray, notifier)
	_:
		return "", fmt.Errorf("unknown transfer method: %s", ingestionTask.TransferMethod)
	}

	if err != nil {
		return datasetId, err
	}

	// mark dataset archivable
	err = datasetIngestor.MarkFilesReady(http_client, SCICAT_API_URL, datasetId, user)
	return datasetId, err
}
