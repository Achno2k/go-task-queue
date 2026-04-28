package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/Achno2k/go-task-queue/internal/database"
	"github.com/Achno2k/go-task-queue/internal/metrics"
	"github.com/Achno2k/go-task-queue/internal/models"
	"github.com/Achno2k/go-task-queue/internal/redis"
)

type Response struct {
	Message string `json:"message"`
	Data    any    `json:"data"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func CreateTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method must be POST to create a task", http.StatusMethodNotAllowed)
		return
	}

	var task models.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if task.Name == "" {
		http.Error(w, "task 'name' is required (must match a registered handler)", http.StatusBadRequest)
		return
	}

	task.ID = uuid.New().String()
	task.Status = string(models.Pending)

	if err := database.DB.Create(&task).Error; err != nil {
		slog.Error("db create failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	taskJSON, _ := json.Marshal(task)
	if err := redis.RDB.LPush(redis.Ctx, redis.MainQueueKey, taskJSON).Err(); err != nil {
		slog.Error("redis enqueue failed", "err", err, "task_id", task.ID)
		http.Error(w, "enqueue failed", http.StatusInternalServerError)
		return
	}

	metrics.TasksEnqueued(task.Name)

	writeJSON(w, http.StatusAccepted, Response{
		Message: "Successfully queued the task",
		Data:    task,
	})
}

func GetTask(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "ID query param is required", http.StatusBadRequest)
		return
	}

	var task models.Task
	if err := database.DB.First(&task, "id = ?", id).Error; err != nil {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, Response{
		Message: "Task retrieved successfully",
		Data:    task,
	})
}

// ListDLQ returns the dead-letter queue contents (newest first).
func ListDLQ(w http.ResponseWriter, r *http.Request) {
	items, err := redis.RDB.LRange(redis.Ctx, redis.DLQKey, 0, 99).Result()
	if err != nil {
		http.Error(w, "redis error", http.StatusInternalServerError)
		return
	}

	tasks := make([]models.Task, 0, len(items))
	for _, raw := range items {
		var t models.Task
		if err := json.Unmarshal([]byte(raw), &t); err == nil {
			tasks = append(tasks, t)
		}
	}

	writeJSON(w, http.StatusOK, Response{
		Message: "DLQ contents",
		Data:    tasks,
	})
}

// RequeueDLQ pulls a specific task out of the DLQ and puts it back on the
// main queue with attempt count reset. Useful for replaying after a bug fix.
func RequeueDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "ID query param is required", http.StatusBadRequest)
		return
	}

	items, err := redis.RDB.LRange(redis.Ctx, redis.DLQKey, 0, -1).Result()
	if err != nil {
		http.Error(w, "redis error", http.StatusInternalServerError)
		return
	}

	for _, raw := range items {
		var t models.Task
		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			continue
		}
		if t.ID != id {
			continue
		}

		t.Attempt = 0
		t.Status = string(models.Pending)
		t.Result = ""
		database.DB.Save(&t)

		newJSON, _ := json.Marshal(t)
		pipe := redis.RDB.TxPipeline()
		pipe.LRem(redis.Ctx, redis.DLQKey, 1, raw)
		pipe.LPush(redis.Ctx, redis.MainQueueKey, newJSON)
		if _, err := pipe.Exec(redis.Ctx); err != nil {
			http.Error(w, "requeue failed", http.StatusInternalServerError)
			return
		}

		writeJSON(w, http.StatusOK, Response{Message: "requeued", Data: t})
		return
	}

	http.Error(w, "not found in DLQ", http.StatusNotFound)
}

// Healthz pings both backends.
func Healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*1e9)
	defer cancel()

	if err := redis.RDB.Ping(ctx).Err(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"redis": err.Error()})
		return
	}
	sqlDB, err := database.DB.DB()
	if err != nil || sqlDB.PingContext(ctx) != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"postgres": "down"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
