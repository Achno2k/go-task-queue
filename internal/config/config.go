package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DBDSN          string
	RedisAddr      string
	HTTPPort       string
	WorkerCount    int
	MaxAttempts    int
	LeaseTimeout   time.Duration
	ReaperInterval time.Duration
}

func Load() Config {
	cfg := Config{
		DBDSN:          getEnvAny([]string{"DB_DSN", "DATABASE_URL"}, "host=localhost user=admin password=password dbname=taskqueue port=5432 sslmode=disable"),
		RedisAddr:      getEnvAny([]string{"REDIS_ADDR", "REDIS_URL"}, "localhost:6379"),
		HTTPPort:       getEnvAny([]string{"HTTP_PORT", "PORT"}, "7070"),
		WorkerCount:    getEnvInt("WORKER_COUNT", 3),
		MaxAttempts:    getEnvInt("MAX_ATTEMPTS", 3),
		LeaseTimeout:   time.Duration(getEnvInt("LEASE_TIMEOUT", 30)) * time.Second,
		ReaperInterval: time.Duration(getEnvInt("REAPER_INTERVAL", 10)) * time.Second,
	}

	log.Printf("config loaded: workers=%d max_attempts=%d lease=%s",
		cfg.WorkerCount, cfg.MaxAttempts, cfg.LeaseTimeout)

	return cfg
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvAny(keys []string, fallback string) string {
	for _, key := range keys {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
