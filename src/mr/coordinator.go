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

type Coordinator struct {
	Files              []string
	NReduce            int
	TaskLock           sync.Mutex
	TaskCondVar        sync.Cond
	IdleMapTasks       map[string]*MapTask
	ProcessingMapTasks map[string]*MapTask
	FinishedMapTasks   map[string]*MapTask

	IdleReduceTasks       map[int]*ReduceTask
	ProcessingReduceTasks map[int]*ReduceTask
	FinishedReduceTasks   map[int]*ReduceTask
	ctx                   context.Context
	cancel                context.CancelFunc
}

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
	log.Printf("Worker %s notified the completion of task of type %s", args.Address, args.TaskType)

	if args.TaskType == MapTaskType {
		log.Printf("map task %s completed by worker %s", args.InputFileName, args.Address)
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
	mapTask, err := c.GetMapTask(args, reply)
	if err != nil {
		log.Printf("Error getting map task for %s: %v", args.PId, err)
		reply.Error = err.Error()
		return nil
	}
	if mapTask != nil {
		log.Printf("Assigned map task with input name: %s and mapTaskNumber: %d \n", mapTask.InputFileName, mapTask.MapTaskNumber)
		return nil
	}
	// If no map task is available, try to get a reduce task
	reduceTask, err := c.GetReduceTask(args, reply)
	if err != nil {
		log.Printf("Error getting reduce task for %s: %v", args.PId, err)
		reply.Error = err.Error()
		return nil
	}

	if reduceTask != nil {
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
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()
	if len(c.FinishedMapTasks) < c.NumMapTask() || len(c.FinishedReduceTasks) < c.NReduce {
		return false
	}
	c.cancel()
	return true
}

func (c *Coordinator) Init(files []string, nReduce int) {
	ctx, cancel := context.WithCancel(context.Background())
	//Initialize the maps
	c.TaskCondVar = *sync.NewCond(&c.TaskLock)
	c.ctx = ctx
	c.cancel = cancel
	c.Files = files
	c.NReduce = nReduce
	c.IdleMapTasks = make(map[string]*MapTask)
	c.ProcessingMapTasks = make(map[string]*MapTask)
	c.FinishedMapTasks = make(map[string]*MapTask)

	c.IdleReduceTasks = make(map[int]*ReduceTask)
	c.ProcessingReduceTasks = make(map[int]*ReduceTask)
	c.FinishedReduceTasks = make(map[int]*ReduceTask)

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
	ticker := time.NewTicker(2 * time.Second)
	go func() {
		defer ticker.Stop() // Always stop ticker when done to avoid leaks

		for {
			select {
			case <-c.ctx.Done():
				return // Exit the goroutine cleanly
			case <-ticker.C:
				c.TaskLock.Lock()
				moveExpiredTasksFromProcessingToIdle(c, c.ProcessingMapTasks, c.IdleMapTasks, c.FinishedMapTasks)
				moveExpiredTasksFromProcessingToIdle(c, c.ProcessingReduceTasks, c.IdleReduceTasks, c.FinishedReduceTasks)
				c.TaskLock.Unlock()
			}
		}
	}()
}

func moveExpiredTasksFromProcessingToIdle[K comparable, V Task](c *Coordinator, from map[K]V, to map[K]V, finished map[K]V) {
	shouldWakeUp := false
	for id, task := range from {
		if task.IsExpired() {
			log.Printf("Task %v is expired, moving it back to idle tasks", id)
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
	registerLogFile("coordinator.log")
	log.Printf("Coordinator starting with %d files and %d reduce tasks", len(files), nReduce)
	c.Init(files, nReduce)
	c.startExpiredTaskCleaner()
	c.server()
	return &c
}
