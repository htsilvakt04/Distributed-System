package kvraft

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Clerk struct {
	clnt             *tester.Clnt
	servers          []string
	activeServerIdx  int32    // index of the last active server
	lookUpServerChan chan int // int is the index of the active server sender know about
	mutex            sync.Mutex
}

func (ck *Clerk) hasActiveServer() bool {
	z := atomic.LoadInt32(&ck.activeServerIdx)
	return z != -1
}

func (ck *Clerk) setActiveServerIdx(idx int32) {
	atomic.StoreInt32(&ck.activeServerIdx, idx)
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt, servers: servers, lookUpServerChan: make(chan int), activeServerIdx: -1,
	}
	go ck.lookUpActiveServer()
	return ck
}

func (ck *Clerk) lookUpActiveServer() {
	for {
		if ck.hasActiveServer() {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		ck.mutex.Lock()
		DPrintf("---- Looking up active server...\n")
		waitGroup := sync.WaitGroup{}
		done := false
		for i := 0; i < len(ck.servers); i++ {
			waitGroup.Add(1)
			go func(index int) {
				defer waitGroup.Done()

				args := rpc.GetStateArgs{}
				reply := rpc.GetStateReply{}
				server := ck.servers[index]
				ok := ck.clnt.Call(server, "KVServer.GetState", &args, &reply)
				DPrintf("GetState() response from server %s: ok=%v, reply=%+v\n", server, ok, reply)
				if done {
					return
				}
				if ok && reply.IsLeader {
					ck.setActiveServerIdx(int32(index))
					DPrintf("Found active server: %s at index %d\n", server, index)
					done = true
					return
				}
			}(i)
		}
		waitGroup.Wait()
		DPrintf("Finish looking up active server, active server index: %d\n", ck.activeServerIdx)
		ck.mutex.Unlock()
	}
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Keep trying until we get a valid response.
	for {
		if !ck.hasActiveServer() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		args := rpc.GetArgs{Key: key, Id: int64(rand.Uint64())}
		reply := rpc.GetReply{}

		ck.mutex.Lock()
		idx := ck.activeServerIdx
		server := ck.servers[idx]
		ck.mutex.Unlock()

		ok := ck.clnt.Call(server, "KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				return reply.Value, reply.Version, rpc.OK
			} else if reply.Err == rpc.ErrNoKey {
				return "", 0, rpc.ErrNoKey
			} else if reply.Err == rpc.ErrWrongLeader {
				if ck.hasActiveServer() {
					ck.setActiveServerIdx(-1)
				}
			}
		} else {
			if ck.hasActiveServer() {
				ck.setActiveServerIdx(-1)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	retry := 0
	for {
		if !ck.hasActiveServer() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		args := rpc.PutArgs{Key: key, Value: value, Version: version, Id: int64(rand.Uint64())}
		reply := rpc.PutReply{}
		DPrintf("---- Clerk.Put() has active server, retry: %d\n", retry)
		ck.mutex.Lock()
		idx := ck.activeServerIdx
		server := ck.servers[idx]
		ck.mutex.Unlock()

		ok := ck.clnt.Call(server, "KVServer.Put", &args, &reply)
		if ok {
			DPrintf("Clerk.Put() put success, with reply: %+v\n", reply)
			if reply.Err == rpc.ErrVersion {
				if retry == 0 {
					return rpc.ErrVersion
				} else {
					return rpc.ErrMaybe
				}
			} else if reply.Err == rpc.ErrWrongLeader {
				if ck.hasActiveServer() {
					ck.setActiveServerIdx(-1)
				}
			} else {
				return reply.Err
			}
		} else {
			if ck.hasActiveServer() {
				ck.setActiveServerIdx(-1)
			}
		}
		retry++
		time.Sleep(10 * time.Millisecond)
	}
}
