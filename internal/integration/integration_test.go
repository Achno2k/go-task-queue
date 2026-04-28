//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Achno2k/go-task-queue/internal/config"
	"github.com/Achno2k/go-task-queue/internal/database"
	"github.com/Achno2k/go-task-queue/internal/models"
	"github.com/Achno2k/go-task-queue/internal/redis"
	"github.com/Achno2k/go-task-queue/internal/worker"
)

// To run:  go test -tags=integration ./internal/integration/...
//
// Requires Docker. The test brings up real Postgres + Redis containers
// and drives a task end-to-end through the worker.

func setup(t *testing.T) (context.Context, config.Config, func()) {
	t.Helper()
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx,
		"postgres:15",
		tcpostgres.WithDatabase("taskqueue"),
		tcpostgres.WithUsername("admin"),
		tcpostgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}

	rc, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	redisURI, err := rc.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis uri: %v", err)
	}

	// connection string looks like redis://host:port — strip the scheme
	addr := redisURI[len("redis://"):]

	database.ConnectDB(dsn)
	redis.InitRedis(addr)

	cfg := config.Config{
		MaxAttempts:    3,
		LeaseTimeout:   3 * time.Second,
		ReaperInterval: 1 * time.Second,
		WorkerCount:    1,
	}

	cleanup := func() {
		_ = pg.Terminate(ctx)
		_ = rc.Terminate(ctx)
	}
	return ctx, cfg, cleanup
}

func enqueue(t *testing.T, name, payload string) string {
	t.Helper()
	task := models.Task{
		ID:      uuid.New().String(),
		Name:    name,
		Payload: payload,
		Status:  string(models.Pending),
	}
	if err := database.DB.Create(&task).Error; err != nil {
		t.Fatalf("db create: %v", err)
	}
	raw, _ := json.Marshal(task)
	if err := redis.RDB.LPush(redis.Ctx, redis.MainQueueKey, raw).Err(); err != nil {
		t.Fatalf("redis push: %v", err)
	}
	return task.ID
}

func TestEndToEndCompletesTask(t *testing.T) {
	ctx, cfg, cleanup := setup(t)
	defer cleanup()

	worker.Register("test.ok", func(ctx context.Context, _ []byte) error { return nil })

	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go worker.StartWorker(wctx, cfg, 1)
	go worker.StartReaper(wctx, cfg)
	go worker.StartSchedular(wctx)

	id := enqueue(t, "test.ok", "{}")

	if !waitForStatus(t, id, "COMPLETED", 10*time.Second) {
		t.Fatalf("task %s did not complete in time", id)
	}
}

func TestRetryThenDLQ(t *testing.T) {
	ctx, cfg, cleanup := setup(t)
	defer cleanup()

	var attempts int32
	worker.Register("test.fail", func(ctx context.Context, _ []byte) error {
		atomic.AddInt32(&attempts, 1)
		return fmt.Errorf("always fails")
	})

	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go worker.StartWorker(wctx, cfg, 1)
	go worker.StartReaper(wctx, cfg)
	go worker.StartSchedular(wctx)

	id := enqueue(t, "test.fail", "{}")

	if !waitForStatus(t, id, "DEAD_LETTER", 30*time.Second) {
		t.Fatalf("task %s did not reach DLQ. attempts=%d", id, atomic.LoadInt32(&attempts))
	}
	if got := atomic.LoadInt32(&attempts); got != int32(cfg.MaxAttempts) {
		t.Errorf("attempts=%d, want %d", got, cfg.MaxAttempts)
	}
}

// TestReaperRequeuesOnCrash simulates a worker crash by manually placing a
// task on a processing list with an already-expired lease, then verifying
// the reaper moves it back to the main queue.
func TestReaperRequeuesOnCrash(t *testing.T) {
	ctx, cfg, cleanup := setup(t)
	defer cleanup()

	id := uuid.New().String()
	task := models.Task{ID: id, Name: "test.ok", Status: string(models.Running)}
	database.DB.Create(&task)
	raw, _ := json.Marshal(task)

	// Pretend worker 99 grabbed it then died.
	procKey := redis.ProcessingKey(99)
	redis.RDB.LPush(ctx, procKey, raw)
	redis.RDB.HSet(ctx, redis.LeaseKey, id, time.Now().Add(-1*time.Second).Unix())

	rctx, stop := context.WithCancel(ctx)
	defer stop()
	go worker.StartReaper(rctx, cfg)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, _ := redis.RDB.LLen(ctx, redis.MainQueueKey).Result()
		if n == 1 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("reaper never requeued the orphan task")
}

func waitForStatus(t *testing.T, id, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var task models.Task
		if err := database.DB.First(&task, "id = ?", id).Error; err == nil {
			if task.Status == want {
				return true
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return false
}
