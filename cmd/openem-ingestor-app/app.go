package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	core "github.com/SwissOpenEM/Ingestor/internal/core"
	metadataextractor "github.com/SwissOpenEM/Ingestor/internal/metadataextractor"
	task "github.com/SwissOpenEM/Ingestor/internal/task"
	webserver "github.com/SwissOpenEM/Ingestor/internal/webserver"

	"github.com/google/uuid"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

type WailsNotifier struct {
	AppContext      context.Context
	loggingNotifier core.LoggingNotifier
}

func (w *WailsNotifier) OnTaskScheduled(id uuid.UUID) {
	w.loggingNotifier.OnTaskScheduled(id)
	runtime.EventsEmit(w.AppContext, "upload-scheduled", id)
}
func (w *WailsNotifier) OnTaskCanceled(id uuid.UUID) {
	w.loggingNotifier.OnTaskCanceled(id)
	runtime.EventsEmit(w.AppContext, "upload-canceled", id)
}
func (w *WailsNotifier) OnTaskAdded(id uuid.UUID, folder string) {
	w.loggingNotifier.OnTaskAdded(id, folder)
	runtime.EventsEmit(w.AppContext, "folder-added", id, folder)
}
func (w *WailsNotifier) OnTaskRemoved(id uuid.UUID) {
	w.loggingNotifier.OnTaskRemoved(id)
	runtime.EventsEmit(w.AppContext, "folder-removed", id)
}
func (w *WailsNotifier) OnTaskFailed(id uuid.UUID, err error) {
	w.loggingNotifier.OnTaskFailed(id, err)
	runtime.EventsEmit(w.AppContext, "upload-failed", id, err.Error())
}
func (w *WailsNotifier) OnTaskCompleted(id uuid.UUID, seconds_elapsed int) {
	w.loggingNotifier.OnTaskCompleted(id, seconds_elapsed)
	runtime.EventsEmit(w.AppContext, "upload-completed", id, seconds_elapsed)
}
func (w *WailsNotifier) OnTaskProgress(id uuid.UUID, percentage float32, elapsedSeconds int) {
	w.loggingNotifier.OnTaskProgress(id, percentage, elapsedSeconds)
	runtime.EventsEmit(w.AppContext, "progress-update", id, percentage, elapsedSeconds)
}

// App struct
type App struct {
	ctx              context.Context
	taskqueue        core.TaskQueue
	config           core.Config
	extractorHandler *metadataextractor.ExtractorHandler
	version          string
}

// NewApp creates a new App application struct
func NewApp(config core.Config, version string) *App {
	return &App{config: config, version: version}
}

// Show prompt before closing the app
func (b *App) beforeClose(ctx context.Context) (prevent bool) {
	dialog, err := runtime.MessageDialog(ctx, runtime.MessageDialogOptions{
		Type:    runtime.QuestionDialog,
		Title:   "Quit?",
		Message: "Are you sure you want to quit? This will stop all pending downloads.",
	})

	if err != nil {
		return false
	}
	return dialog != "Yes"
}

// startup is called when the app starts. The context is saved
// so we can call the runtime methods
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx

	a.extractorHandler = metadataextractor.NewExtractorHandler(a.config.MetadataExtractors)

	a.taskqueue = core.TaskQueue{Config: a.config,
		AppContext: a.ctx,
		Notifier:   &WailsNotifier{AppContext: a.ctx},
	}
	a.taskqueue.Startup()

	go func(port int) {
		ingestor, err := webserver.NewIngestorWebServer(a.version, &a.taskqueue, a.extractorHandler, a.config.WebServer)
		if err != nil {
			panic(err)
		}
		s := webserver.NewIngesterServer(ingestor, port)
		log.Fatal(s.ListenAndServe())
	}(a.config.Misc.Port)

}

func (a *App) SelectFolder() {
	folder, err := task.SelectFolder(a.ctx)
	if err != nil || folder.FolderPath == "" {
		return
	}

	err = a.taskqueue.CreateTaskFromDatasetFolder(folder)
	if err != nil {
		return
	}
}

func (a *App) ExtractMetadata(extractor_name string, id uuid.UUID) string {

	folder := a.taskqueue.GetTaskFolder(id)
	if folder == "" {
		return ""
	}

	log_message := func(id uuid.UUID, msg string) {
		slog.Info("Extractor output: ", "message", msg)
		runtime.EventsEmit(a.ctx, "log-update", id, msg)
	}

	log_error := func(id uuid.UUID, msg string) {
		slog.Info("Extractor error: ", "message", msg)
		runtime.EventsEmit(a.ctx, "log-update", id, msg)
	}

	outputfile := metadataextractor.MetadataFilePath(folder)

	metadata, err := a.extractorHandler.ExtractMetadata(a.ctx, extractor_name, folder, outputfile, func(message string) { log_message(id, message) }, func(message string) { log_error(id, message) })

	if err != nil {
		slog.Error("Metadata extraction failed", "error", err.Error())
		return fmt.Sprintf("{\"status\":\"%s\"}", err.Error())
	}
	return metadata
}

type ExtractionMethod struct {
	Name   string
	Schema string
}

func (e *ExtractionMethod) GetName() string {
	return e.Name
}

func (e *ExtractionMethod) GetSchema() string {
	return e.Schema
}

func (a *App) AvailableMethods() []ExtractionMethod {
	e := []ExtractionMethod{}
	for _, ex := range a.extractorHandler.AvailableMethods() {
		e = append(e, ExtractionMethod{
			Name:   ex.Name,
			Schema: ex.Schema,
		})
	}
	return e
}

func (a *App) CancelTask(id uuid.UUID) {
	a.taskqueue.CancelTask(id)
}
func (a *App) RemoveTask(id uuid.UUID) {
	err := a.taskqueue.RemoveTask(id)
	log.Println(err)
}

func (a *App) ScheduleTask(id uuid.UUID) {

	a.taskqueue.ScheduleTask(id)
}
