package mr

import (
	"log"
	"time"
)

func (c *Coordinator) GetMapTask(args *GetTaskArgs, reply *GetTaskReply) (*MapTask, error) {
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()
	// Prioritize reduce tasks
	for len(c.FinishedMapTasks) < c.NumMapTask() {
		// have no idle tasks, then sleep
		if len(c.IdleMapTasks) == 0 {
			log.Printf("No idle map tasks available, worker %s will wait", args.Address)
			c.TaskCondVar.Wait()
		} else {
			var task *MapTask
			for _, t := range c.IdleMapTasks {
				task = t
				break
			}
			if task == nil {
				log.Printf("Internal error: no idle map tasks found for worker %s", args.Address)
				panic("No idle map tasks found")
			}
			// remove from idle tasks
			delete(c.IdleMapTasks, task.InputFileName)

			// move to processing tasks
			task.ProcessedTime = time.Now().Unix()
			c.ProcessingMapTasks[task.InputFileName] = task

			reply.TaskType = MapTaskType
			reply.MapTask = task
			reply.ReduceTask = nil
			log.Printf("Assigned map task %d to %s", task.MapTaskNumber, args.Address)
			return task, nil
		}
	}
	return nil, nil
}
func (c *Coordinator) GetReduceTask(args *GetTaskArgs, reply *GetTaskReply) (*ReduceTask, error) {
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()
	// Prioritize reduce tasks
	for len(c.FinishedReduceTasks) < c.NReduce {
		// have no idle tasks, then sleep
		if len(c.IdleReduceTasks) == 0 {
			log.Printf("No idle reduce tasks available, worker %s will wait", args.Address)
			c.TaskCondVar.Wait()
		} else {
			var task *ReduceTask
			for _, t := range c.IdleReduceTasks {
				task = t
				break
			}
			if task == nil {
				log.Printf("Internal error: no idle map tasks found for worker %s", args.Address)
				panic("No idle map tasks found")
			}

			task.ProcessedTime = time.Now().Unix()
			// remove from idle tasks
			delete(c.IdleReduceTasks, task.ReduceTaskNumber)
			// move to processing tasks
			c.ProcessingReduceTasks[task.ReduceTaskNumber] = task

			reply.TaskType = ReduceTaskType
			reply.MapTask = nil
			reply.ReduceTask = task
			log.Printf("Assigned reduce task %d to %s", task.ReduceTaskNumber, args.Address)
			return task, nil
		}
	}
	return nil, nil
}
