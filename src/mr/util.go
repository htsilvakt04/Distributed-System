package mr

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
)

func ReadKVFromFile(taskLock *sync.Mutex, filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	taskLock.Lock()
	defer taskLock.Unlock()
	kva := mapf(filename, string(content))
	return kva
}
