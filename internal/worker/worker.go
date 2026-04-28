package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/Achno2k/go-task-queue/internal/config"
	"github.com/Achno2k/go-task-queue/internal/database"
	"github.com/Achno2k/go-task-queue/internal/metrics"
	"github.com/Achno2k/go-task-queue/internal/models"
	"github.com/Achno2k/go-task-queue/internal/redis"
	r "github.com/redis/go-redis/v9"
)

// StartWorker pulls tasks off the main queue using BLMove so the claim is
// atomic — the task lands on a per-worker "processing" list at the same
// instant it leaves the main queue. Coupled with the lease entry in
// task_leases, this is how we get at-least-once delivery without two
// workers ever fighting over the same task.
func StartWorker(ctx context.Context, cfg config.Config, workerID int) {
	processingKey := redis.ProcessingKey(workerID)
	log := slog.With("worker_id", workerID)

	// On startup, requeue anything left in our processing list from a
	// previous incarnation of this worker (clean restart, not a crash).
	requeueOrphans(ctx, processingKey, log)

	for {
		if ctx.Err() != nil {
			log.Info("worker stopping")
			return
		}

		// BLMove blocks for up to 2s, then returns nil so we can re-check ctx.
		taskJSON, err := redis.RDB.BLMove(
			ctx,
			redis.MainQueueKey,
			processingKey,
			"LEFT", "RIGHT",
			2*time.Second,
		).Result()

		if err != nil {
			if errors.Is(err, r.Nil) || ctx.Err() != nil {
				continue
			}
			log.Error("BLMove failed", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var task models.Task
		if err := json.Unmarshal([]byte(taskJSON), &task); err != nil {
			log.Error("bad task json, dropping", "err", err, "raw", taskJSON)
			redis.RDB.LRem(ctx, processingKey, 1, taskJSON)
			continue
		}

		processOne(ctx, cfg, log, processingKey, taskJSON, &task)
	}
}

func processOne(ctx context.Context, cfg config.Config, log *slog.Logger, processingKey, taskJSON string, task *models.Task) {
	tlog := log.With("task_id", task.ID, "name", task.Name, "attempt", task.Attempt)
	start := time.Now()

	// Take the lease. If we crash, the reaper will see this expire and
	// requeue the task.
	leaseExpiry := time.Now().Add(cfg.LeaseTimeout).Unix()
	if err := redis.RDB.HSet(ctx, redis.LeaseKey, task.ID, leaseExpiry).Err(); err != nil {
		tlog.Error("failed to set lease", "err", err)
	}

	task.Status = string(models.Running)
	database.DB.Save(task)
	metrics.InflightInc()
	defer metrics.InflightDec()

	tlog.Info("processing")

	handler, ok := lookup(task.Name)
	var runErr error
	if !ok {
		runErr = fmt.Errorf("no handler registered for %q", task.Name)
	} else {
		runErr = safeRun(ctx, handler, []byte(task.Payload))
	}

	dur := time.Since(start)
	metrics.ObserveDuration(task.Name, dur)

	if runErr != nil {
		handleFailure(ctx, cfg, tlog, processingKey, taskJSON, task, runErr)
		return
	}

	task.Status = string(models.Completed)
	task.Result = "ok"
	database.DB.Save(task)

	ackTask(ctx, processingKey, taskJSON, task.ID)
	metrics.TasksProcessed("completed", task.Name)
	tlog.Info("completed", "duration_ms", dur.Milliseconds())
}

func handleFailure(ctx context.Context, cfg config.Config, log *slog.Logger, processingKey, taskJSON string, task *models.Task, runErr error) {
	task.Attempt++
	log = log.With("err", runErr.Error(), "next_attempt", task.Attempt)

	if task.Attempt >= cfg.MaxAttempts {
		task.Status = string(models.DeadLetter)
		task.Result = "exhausted retries: " + runErr.Error()
		database.DB.Save(task)

		if dlqJSON, err := json.Marshal(task); err == nil {
			redis.RDB.LPush(ctx, redis.DLQKey, dlqJSON)
		}
		ackTask(ctx, processingKey, taskJSON, task.ID)
		metrics.TasksProcessed("dead_letter", task.Name)
		log.Warn("task moved to DLQ")
		return
	}

	// Exponential backoff with jitter so retries don't thunder.
	base := int64(1 << task.Attempt) // 2, 4, 8, ...
	jitter := rand.Int63n(base)      // up to base seconds
	delay := base + jitter

	task.Status = string(models.Pending)
	task.NextRunAt = time.Now().Unix() + delay
	database.DB.Save(task)

	newJSON, _ := json.Marshal(task)
	redis.RDB.ZAdd(ctx, redis.DelayQueueKey, r.Z{
		Score:  float64(task.NextRunAt),
		Member: newJSON,
	})

	ackTask(ctx, processingKey, taskJSON, task.ID)
	metrics.TasksProcessed("retried", task.Name)
	log.Info("scheduled retry", "delay_s", delay)
}

// ackTask removes the in-flight task from the processing list and clears
// its lease. Called whether the task succeeded, was retried, or hit DLQ.
func ackTask(ctx context.Context, processingKey, taskJSON, taskID string) {
	redis.RDB.LRem(ctx, processingKey, 1, taskJSON)
	redis.RDB.HDel(ctx, redis.LeaseKey, taskID)
}

// safeRun catches handler panics so one bad handler can't kill the worker.
func safeRun(ctx context.Context, h Handler, payload []byte) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("handler panic: %v", rec)
		}
	}()
	return h(ctx, payload)
}

// requeueOrphans handles a clean restart: anything in our processing list
// belongs to the previous incarnation of this worker ID. Push it back onto
// the main queue. (Crashes are handled separately by the reaper, which
// works across all worker IDs via lease expiry.)
func requeueOrphans(ctx context.Context, processingKey string, log *slog.Logger) {
	for {
		taskJSON, err := redis.RDB.RPop(ctx, processingKey).Result()
		if err != nil {
			return
		}
		redis.RDB.LPush(ctx, redis.MainQueueKey, taskJSON)
		log.Info("requeued orphan from previous run")
	}
}
