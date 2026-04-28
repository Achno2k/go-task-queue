package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Achno2k/go-task-queue/internal/config"
	"github.com/Achno2k/go-task-queue/internal/database"
	"github.com/Achno2k/go-task-queue/internal/handlers"
	"github.com/Achno2k/go-task-queue/internal/metrics"
	"github.com/Achno2k/go-task-queue/internal/queue"
	"github.com/Achno2k/go-task-queue/internal/redis"
	"github.com/Achno2k/go-task-queue/internal/worker"
)

func main() {
	// JSON logs in prod would be nicer; text is friendlier when you're
	// reading docker compose output by eye.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := config.Load()

	database.ConnectDB(cfg.DBDSN)
	redis.InitRedis(cfg.RedisAddr)

	worker.RegisterDefaults()
	queue.RecoverTasks()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() { defer wg.Done(); worker.StartSchedular(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); worker.StartReaper(ctx, cfg) }()

	wg.Add(1)
	go func() { defer wg.Done(); metrics.StartDepthSampler(ctx) }()

	for i := 1; i <= cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); worker.StartWorker(ctx, cfg, id) }(i)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/tasks", handlers.CreateTask)
	mux.HandleFunc("/task", handlers.GetTask)
	mux.HandleFunc("/tasks/dlq", handlers.ListDLQ)
	mux.HandleFunc("/tasks/dlq/requeue", handlers.RequeueDLQ)
	mux.HandleFunc("/healthz", handlers.Healthz)
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		slog.Info("http server listening", "port", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server died", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received, draining...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown error", "err", err)
	}

	wg.Wait()
	slog.Info("goodbye")
}
