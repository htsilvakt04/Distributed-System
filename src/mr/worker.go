package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

		// process the task
		if reply.TaskType == MapTaskType {
			handleMapTask(reply.MapTask, mapf)
			notifyTaskSuccess(MapTaskType, reply.MapTask.InputFileName, 0)
		}
		if reply.TaskType == ReduceTaskType {
			handleReduceTask(reply.ReduceTask, reducef)
			notifyTaskSuccess(ReduceTaskType, "", reply.ReduceTask.ReduceTaskNumber)
		}
	} // end for loop
}

func notifyTaskSuccess(taskType TaskType, mapFile string, reduceNum int) {
	args := NotifyTaskSuccessArgs{
		TaskType:         taskType,
		InputFileName:    mapFile,
		ReduceTaskNumber: reduceNum,
		Address:          coordinatorSock(),
	}
	reply := NotifyTaskSuccessReply{}
	ok := call("Coordinator.NotifyTaskSuccess", &args, &reply)
	if !ok {
		DPrintf("Failed to notify coordinator about task success for task type %s, map file %s, reduce number %d", taskType, mapFile, reduceNum)
	} else {
		DPrintf("Successfully notified coordinator about task success for task type %s, map file %s, reduce number %d", taskType, mapFile, reduceNum)
	}
}
func handleMapTask(task *MapTask, mapf func(string, string) []KeyValue) {
	// read from input file, to create a list of kv pairs
	kvs := ReadKVFromFile(task.InputFileName, mapf)
	// reduce the list of kv pairs to a map
	kvsMap := make(map[string][]KeyValue)
	for _, kv := range kvs {
		kvsMap[kv.Key] = append(kvsMap[kv.Key], kv)
	}
	fileContentMap := make(map[string][]KeyValue)

	// fill the fileContentMap with the kvsMap
	for key, values := range kvsMap {
		// get the reduce task number for this key
		reduceTaskNumber := ihash(key) % task.NumReducer
		outputFileName := fmt.Sprintf("mr-%d-%d", task.MapTaskNumber, reduceTaskNumber)
		fileContentMap[outputFileName] = append(fileContentMap[outputFileName], values...)
	}

	DPrintf("Map task %d processed input file %s, outputting to %d files", task.MapTaskNumber, task.InputFileName, len(fileContentMap))
	// write the fileContentMap to output files
	for outputFileName, values := range fileContentMap {
		file, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			DPrintf("cannot open %v", outputFileName)
		}
		enc := json.NewEncoder(file)
		if err := enc.Encode(&values); err != nil {
			DPrintf("cannot write to %v: %v", outputFileName, err)
		}
		if err := file.Close(); err != nil {
			DPrintf("cannot close %v: %v", outputFileName, err)
		}
	}
}

func handleReduceTask(task *ReduceTask, reducef func(string, []string) string) {
	// collect all files that match the pattern mr-*-<task.ReduceTaskNumber> to intermediate
	var intermediate []KeyValue
	if !collectKVs(task, &intermediate) {
		DPrintf("Failed to collect key-value pairs for reduce task %d", task.ReduceTaskNumber)
		return
	}
	// sort them
	sort.Sort(ByKey(intermediate))

	outFileName := fmt.Sprintf("mr-out-%d", task.ReduceTaskNumber)
	// avoid partial writes by using temporary file
	tempFile, err := os.CreateTemp("", "mr-reduce-")
	if err != nil {
		DPrintf("cannot create temporary file for reduce output: %v", err)
		return
	}

	defer func(tempFile *os.File) {
		if err := tempFile.Close(); err != nil {
			log.Fatalf("cannot close temporary file: %v", err)
		}
	}(tempFile)

	// Accumulate the values for each key, then call reducef
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// write the output to the temporary file
		if _, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output); err != nil {
			DPrintf("cannot write to temporary file: %v", err)
		}
		i = j
	}

	if err := os.Rename(tempFile.Name(), outFileName); err != nil {
		DPrintf("cannot rename temporary file to output file %s: %v", outFileName, err)
	} else {
		DPrintf("Reduce task %d successfully wrote output to file %s", task.ReduceTaskNumber, outFileName)
	}
}

func collectKVs(task *ReduceTask, kva *[]KeyValue) bool {
	// collect all files that match the pattern mr-*-<task.ReduceTaskNumber>
	// and read them into kva
	for i := 0; i < task.TotalNumberOfMapTasks; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.ReduceTaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}

				DPrintf("cannot decode %v", filename)
			}
			*kva = append(*kva, kv)
		}
		if err := file.Close(); err != nil {
			log.Fatalf("cannot close %v: %v", filename, err)
			return false
		}
	}

	return true
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
