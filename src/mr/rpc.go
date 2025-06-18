package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

type GetTaskReply struct {
	TaskType   TaskType    // enum: map or reduce
	MapTask    *MapTask    // non-nil if TaskType == MapTaskType
	ReduceTask *ReduceTask // non-nil if TaskType == ReduceTaskType
	Done       bool        // true if there are no more tasks to assign
	Error      string
}

type GetTaskArgs struct {
	Address string
}

type NotifyTaskSuccessArgs struct {
	TaskType         TaskType
	InputFileName    string // valid only if TaskType == MapTaskType
	ReduceTaskNumber int    // alid only if TaskType == ReduceTaskType
	Address          string
}
type NotifyTaskSuccessReply struct{}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
