package core

import (
	"log/slog"

	"github.com/google/uuid"
)

type LoggingNotifier struct{}

func (n *LoggingNotifier) OnTaskScheduled(id uuid.UUID) {
	slog.Info("Task scheduled", "id", id)
}
func (n *LoggingNotifier) OnTaskCanceled(id uuid.UUID) {
	slog.Info("Task canceled", "id", id)
}
func (n *LoggingNotifier) OnTaskAdded(id uuid.UUID, folder string) {
	slog.Info("Task added", "id", id, "folder", folder)
}
func (n *LoggingNotifier) OnTaskRemoved(id uuid.UUID) {
	slog.Info("Task removed", "id", id)
}
func (n *LoggingNotifier) OnTaskFailed(id uuid.UUID, err error) {
	slog.Error("Task failed", "id", id, "err", err.Error())
}
func (n *LoggingNotifier) OnTaskCompleted(id uuid.UUID, elapsedSeconds int) {
	slog.Info("Task completed", "id", id, "elapsed", elapsedSeconds)
}
func (n *LoggingNotifier) OnTaskProgress(id uuid.UUID, percentage float32, elapsedSeconds int) {
	slog.Info("Task progress", "id", id, "percentage", percentage, "elapsed", elapsedSeconds)
}
