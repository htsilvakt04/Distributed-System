package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// CallExample()
	args := GetTaskArgs{Address: coordinatorSock()}
	retryCount := 0
	for {
		reply := GetTaskReply{}
		DPrintf("worker calling GetTask with args: %+v", args)
		ok := call("Coordinator.GetTask", &args, &reply)

		if !ok {
			retryCount++
			if retryCount > 2 {
				DPrintf("Shutting down worker after retrying 2 times to get a task")
				return
			}

			DPrintf("GetTask failed, retrying... (%d)", retryCount)
			time.Sleep(time.Second)
			continue
		}
		// reset retry count on successful call
		retryCount = 0
		if reply.Done {
			DPrintf("No more tasks available, worker shutting down")
			return
		}
		// unrecoverable error from server
		if reply.Error != "" {
			DPrintf("Error from server %s", reply.Error)
			return
		}
		DPrintf("Process %s task type \n", reply.TaskType)
		// process the task
		if reply.TaskType == MapTaskType {
			handleMapTask(reply.MapTask, mapf)
		}
		if reply.TaskType == ReduceTaskType {
			handleReduceTask(reply.ReduceTask, reducef)
		}
	} // end for loop
}

func handleMapTask(task *MapTask, mapf func(string, string) []KeyValue) {
	time.Sleep(time.Second)
	call()
}

func handleReduceTask(task *ReduceTask, reducef func(string, []string) string) {

}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
