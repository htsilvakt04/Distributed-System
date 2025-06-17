package mr

import "time"

func (c *Coordinator) GetMapTask(args *GetTaskArgs, reply *GetTaskReply) (*MapTask, error) {
	c.TaskLock.Lock()
	defer c.TaskLock.Unlock()
	// Prioritize reduce tasks
	for len(c.FinishedMapTasks) < c.NumMapTask() {
		// have no idle tasks, then sleep
		if len(c.IdleMapTasks) == 0 {
			DPrintf("No idle map tasks available, worker %s will wait", args.Address)
			c.TaskCondVar.Wait()
		} else {
			task := c.IdleMapTasks[0]
			task.ProcessedTime = time.Now().Unix()
			// remove from idle tasks
			c.IdleMapTasks = c.IdleMapTasks[1:]
			// move to processing tasks
			c.ProcessingMapTasks = append(c.ProcessingMapTasks, task)

			reply.TaskType = MapTaskType
			reply.MapTask = task
			reply.ReduceTask = nil
			DPrintf("Assigned map task %d to %s", task.MapTaskNumber, args.Address)
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
			DPrintf("No idle reduce tasks available, worker %s will wait", args.Address)
			c.TaskCondVar.Wait()
		} else {
			task := c.IdleReduceTasks[0]
			task.ProcessedTime = time.Now().Unix()
			// remove from idle tasks
			c.IdleReduceTasks = c.IdleReduceTasks[1:]
			// move to processing tasks
			c.ProcessingReduceTasks = append(c.ProcessingReduceTasks, task)

			reply.TaskType = ReduceTaskType
			reply.MapTask = nil
			reply.ReduceTask = task
			DPrintf("Assigned reduce task %d to %s", task.ReduceTaskNumber, args.Address)
			return task, nil
		}
	}
	return nil, nil
}

func removeExpiredTasks[T Task](from *[]T, to *[]T) {
	tasks := *from
	i := 0
	for i < len(tasks) {
		task := tasks[i]
		if task.IsExpired() {
			*to = append(*to, task)
			tasks = append(tasks[:i], tasks[i+1:]...)
		} else {
			i++
		}
	}
	*from = tasks
}
