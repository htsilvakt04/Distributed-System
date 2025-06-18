package mr

import "time"

const EXPIRED_TASK_TIMEOUT = 10 // seconds

type TaskType string

const (
	MapTaskType    TaskType = "map"
	ReduceTaskType          = "reduce"
)

type Task interface {
	IsExpired() bool
	GetProcessedTime() int64
	resetProcessedTime()
}

func (t *MapTask) resetProcessedTime() {
	t.ProcessedTime = 0
}
func (t *ReduceTask) resetProcessedTime() {
	t.ProcessedTime = 0
}
func (t *MapTask) IsExpired() bool {
	if t.ProcessedTime == 0 {
		return false
	}
	return time.Now().Unix()-t.ProcessedTime > EXPIRED_TASK_TIMEOUT
}

func (t *MapTask) GetProcessedTime() int64 {
	return t.ProcessedTime
}
func (t *ReduceTask) IsExpired() bool {
	if t.ProcessedTime == 0 {
		return false
	}
	return time.Now().Unix()-t.ProcessedTime > EXPIRED_TASK_TIMEOUT
}

func (t *ReduceTask) GetProcessedTime() int64 {
	return t.ProcessedTime
}

type MapTask struct {
	InputFileName string // id
	NumReducer    int
	MapTaskNumber int
	ProcessedTime int64 // epoch time
}
type ReduceTask struct {
	ReduceTaskNumber      int // id
	TotalNumberOfMapTasks int
	ProcessedTime         int64 // epoch time
}
