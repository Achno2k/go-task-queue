package queue

import (
	"encoding/json"
	"log/slog"

	"github.com/Achno2k/go-task-queue/internal/database"
	"github.com/Achno2k/go-task-queue/internal/models"
	"github.com/Achno2k/go-task-queue/internal/redis"
)

// RecoverTasks runs once at startup. Anything left in PENDING or RUNNING
// from a previous run is pushed back onto the main queue so a worker can
// pick it up. RUNNING gets reset to PENDING because whichever worker had
// it is gone.
func RecoverTasks() {
	var tasks []models.Task

	result := database.DB.Where("status in ?", []string{"PENDING", "RUNNING"}).Find(&tasks)
	if result.Error != nil {
		slog.Error("recover: query failed", "err", result.Error)
		return
	}

	for _, task := range tasks {
		if task.Status == string(models.Running) {
			task.Status = string(models.Pending)
			database.DB.Save(&task)
		}
		taskJSON, _ := json.Marshal(task)
		redis.RDB.LPush(redis.Ctx, redis.MainQueueKey, taskJSON)
	}

	slog.Info("recovered tasks from previous run", "count", len(tasks))
}
