package models

type status string

const (
	Pending   status = "PENDING"
	Running   status = "RUNNING"
	Completed status = "COMPLETED"
	Failed    status = "FAILED"
	DeadLetter status = "DEAD_LETTER"
)

type Task struct {
	ID        string `gorm:"primaryKey" json:"id"`
	Name      string `json:"name"`
	Payload   string `json:"payload"`
	Status    string `json:"status"`
	Result    string `json:"result"`
	Attempt   int    `json:"attempt"`
	Priority  int    `json:"priority"`
	NextRunAt int64  `json:"next_run_at"`
}
