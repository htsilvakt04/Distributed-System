package mr

import "time"

const EXPIRED_TASK_TIMEOUT = 15

type TaskType string

const (
	MapTaskType    TaskType = "map"
	ReduceTaskType          = "reduce"
)

type Task interface {
	IsExpired() bool
	GetProcessedTime() int64
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
func (t *MapTask) GetWorkerPid() string {
	return t.WorkerPId
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
func (t *ReduceTask) GetWorkerPid() string {
	return t.WorkerPId
}

type MapTask struct {
	InputFileName string // id
	NumReducer    int
	MapTaskNumber int
	ProcessedTime int64  // epoch time
	WorkerPId     string // worker process id
}
type ReduceTask struct {
	ReduceTaskNumber      int // id
	TotalNumberOfMapTasks int
	ProcessedTime         int64  // epoch time
	WorkerPId             string // worker process id
}
