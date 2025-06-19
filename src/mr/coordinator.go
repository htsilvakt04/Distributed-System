package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "context"

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type Coordinator struct {
	Files   []string
	NReduce int

	TaskLock           sync.Mutex
	TaskCondVar        sync.Cond
	IdleMapTasks       map[string]*MapTask
	ProcessingMapTasks map[string]*MapTask
	FinishedMapTasks   map[string]*MapTask

	IdleReduceTasks       map[int]*ReduceTask
	ProcessingReduceTasks map[int]*ReduceTask
	FinishedReduceTasks   map[int]*ReduceTask

	ctx    context.Context
	cancel context.CancelFunc
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) NumMapTask() int {
	return len(c.Files)
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) NotifyTaskSuccess(args *NotifyTaskSuccessArgs, reply *NotifyTaskSuccessReply) error {
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()
	DPrintf("Worker %s notified the completion of task of type %s", args.Address, args.TaskType)

	if args.TaskType == MapTaskType {
		moveProcessingTaskToFinish[string, *MapTask](c, args.InputFileName, c.ProcessingMapTasks, c.FinishedMapTasks)
	}
	if args.TaskType == ReduceTaskType {
		moveProcessingTaskToFinish[int, *ReduceTask](c, args.ReduceTaskNumber, c.ProcessingReduceTasks, c.FinishedReduceTasks)
	}
	return nil
}

func moveProcessingTaskToFinish[K comparable, V Task](c *Coordinator, taskId K, processingMap map[K]V, finishMap map[K]V) {
	for id, task := range processingMap {
		if taskId == id {
			// move to finished tasks
			finishMap[id] = task
			// remove from processing tasks
			delete(processingMap, id)
			c.TaskCondVar.Broadcast() // notify waiting workers
		}
	}
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	DPrintf("Process %s is requesting a task", args.Address)
	map_task, err := c.GetMapTask(args, reply)
	if err != nil {
		DPrintf("Error getting map task for %s: %v", args.Address, err)
		reply.Error = err.Error()
		return nil
	}
	if map_task != nil {
		return nil
	}
	// If no map task is available, try to get a reduce task
	reduce_task, err := c.GetReduceTask(args, reply)
	if err != nil {
		DPrintf("Error getting reduce task for %s: %v", args.Address, err)
		reply.Error = err.Error()
		return nil
	}
	if reduce_task != nil {
		return nil
	}
	// If no tasks are available, set Done to true
	reply.Done = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true
	if ret {
		c.cancel()
	}
	// Your code here.

	return ret
}

func (c *Coordinator) Init(files []string, nReduce int) {
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel

	// Initialize idle map tasks
	for i, file := range files {
		task := MapTask{
			InputFileName: file,
			NumReducer:    nReduce,
			MapTaskNumber: i,
			ProcessedTime: 0,
		}
		c.IdleMapTasks[file] = &task
	}
	// Initialize idle reduce tasks
	for i := range nReduce {
		task := ReduceTask{
			ReduceTaskNumber:      i,
			TotalNumberOfMapTasks: len(files),
			ProcessedTime:         0,
		}
		c.IdleReduceTasks[i] = &task
	}
}

func (c *Coordinator) startExpiredTaskCleaner() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		defer ticker.Stop() // Always stop ticker when done to avoid leaks

		for {
			select {
			case <-c.ctx.Done():
				return // Exit the goroutine cleanly
			case <-ticker.C:
				c.TaskLock.Lock()
				moveExpiredTasksFromProcessingToIdle(c, c.ProcessingMapTasks, c.IdleMapTasks)
				moveExpiredTasksFromProcessingToIdle(c, c.ProcessingReduceTasks, c.IdleReduceTasks)
				c.TaskLock.Unlock()
			}
		}
	}()
}

func moveExpiredTasksFromProcessingToIdle[K comparable, V Task](c *Coordinator, from map[K]V, to map[K]V) {
	shouldWakeUp := false
	for id, task := range from {
		if task.IsExpired() {
			to[id] = task
			delete(from, id)
			shouldWakeUp = true
		}
	}

	if shouldWakeUp {
		c.TaskCondVar.Broadcast()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Init(files, nReduce)
	c.startExpiredTaskCleaner()
	c.server()
	return &c
}
