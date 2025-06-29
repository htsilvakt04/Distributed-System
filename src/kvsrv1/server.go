package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu    sync.Mutex
	table map[string]*Value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.table = make(map[string]*Value)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.table[args.Key]; ok {
		reply.Value = value.Value
		reply.Version = value.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.table[args.Key]

	if !ok {
		if args.Version == 0 {
			// Key does not exist, but version is 0, so we can create it.
			kv.table[args.Key] = &Value{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
			return
		}

		reply.Err = rpc.ErrNoKey
		return
	}

	if args.Version != value.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	// update the value
	value.Version++
	value.Value = args.Value
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
