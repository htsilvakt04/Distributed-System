package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me           int
	dead         int32 // set by Kill()
	rsm          *rsm.RSM
	table        map[string]*Value
	processedOps map[int64]any
	mu           sync.Mutex
}
type Value struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch op := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		kv.get(&op, &reply)
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}
		kv.put(&op, &reply)
		return reply
	default:
		log.Printf("DoOp: unsupported operation type: %T\n", req)
		return "Not supported operation"
	}
}

func (kv *KVServer) Snapshot() []byte {
	buff := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buff)
	data := make(map[string]Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for key, value := range kv.table {
		data[key] = Value{
			Value:   value.Value,
			Version: value.Version,
		}
	}
	err := encoder.Encode(data)
	if err != nil {
		log.Printf("Snapshot Error: %v\n", err)
		panic(err)
	}

	return buff.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	table := make(map[string]Value)
	if err := decoder.Decode(&table); err != nil {
		log.Printf("Restore Error: %v\n", err)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.table = make(map[string]*Value)
	for key, value := range table {
		kv.table[key] = &Value{
			Value:   value.Value,
			Version: value.Version,
		}
	}
	DPrintf("[%d] Restore() called, restored table of size: %v\n", kv.me, len(table))
}

func (kv *KVServer) GetState(args *rpc.GetStateArgs, reply *rpc.GetStateReply) {
	_, isLeader := kv.rsm.Raft().GetState()
	DPrintf("[%d] GetState() called, isLeader: %t\n", kv.me, isLeader)
	reply.IsLeader = isLeader
}

func (kv *KVServer) get(args *rpc.GetArgs, reply *rpc.GetReply) {
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
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	// check if the operation has been processed before
	if kv.handleDeduplication(args, reply) {
		return
	}

	err, data := kv.rsm.Submit(*args)
	if data != nil {
		convertData := data.(rpc.GetReply)
		reply.Value = convertData.Value
		reply.Version = convertData.Version
	}
	kv.mu.Lock()
	kv.processedOps[args.Id] = reply
	DPrintf("[%d] Get() return with err %v, value %s, version %d\n", kv.me, err, reply.Value, reply.Version)
	kv.mu.Unlock()
	reply.Err = err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	if kv.handleDeduplication(args, reply) {
		return
	}
	wrongLeaderErr, rep := kv.rsm.Submit(*args)
	reply.Err = wrongLeaderErr
	if wrongLeaderErr != rpc.ErrWrongLeader {
		reply.Err = rep.(rpc.PutReply).Err
	}
	kv.mu.Lock()
	kv.processedOps[args.Id] = reply
	DPrintf("[%d] Put() return with err %v\n", kv.me, reply.Err)
	kv.mu.Unlock()
}
func (kv *KVServer) put(args *rpc.PutArgs, reply *rpc.PutReply) {
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handleDeduplication(args any, reply any) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch operation := args.(type) {
	case *rpc.GetArgs:
		DPrintf("[%d] Get() with args %v\n", kv.me, *operation)
		if kv.processedOps[operation.Id] != nil {
			op := kv.processedOps[operation.Id].(*rpc.GetReply)
			rep := reply.(*rpc.GetReply)
			rep.Value = op.Value
			rep.Version = op.Version
			rep.Err = op.Err
			return true
		}
		return false
	case *rpc.PutArgs:
		DPrintf("[%d] Put() with args %v\n", kv.me, *operation)
		if kv.processedOps[operation.Id] != nil {
			op := kv.processedOps[operation.Id].(*rpc.PutReply)
			rep := reply.(*rpc.GetReply)
			rep.Err = op.Err
			kv.mu.Unlock()
			return true
		}
		return false
	default:
		log.Printf("Unknow operation: %s", operation)
		return false
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(rpc.GetStateArgs{})
	labgob.Register(rpc.GetStateReply{})

	kv := &KVServer{me: me, table: make(map[string]*Value), processedOps: make(map[int64]any)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
