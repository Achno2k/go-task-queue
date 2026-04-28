package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const (
	MainQueueKey      = "task_queue"
	DelayQueueKey     = "delay_queue"
	DLQKey            = "task_queue:dlq"
	LeaseKey          = "task_leases"
	ProcessingPrefix  = "processing:"
)

var Ctx = context.Background()
var RDB *redis.Client

func ProcessingKey(workerID int) string {
	return ProcessingPrefix + itoa(workerID)
}

func InitRedis(addr string) {
	RDB = redis.NewClient(&redis.Options{
		Addr: addr,
	})
}

// small helper to keep this file dependency-free
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
