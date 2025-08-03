package rsm

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Id  int64       // unique operation ID
	Me  int         // who submitted
	Req interface{} // the actual application-level command
}
type PendingOp struct {
	id       int64 // unique operation ID
	retValue any
	retErr   rpc.Err
	finished bool
	index    int // index of the op in the log of raft
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}
type RSM struct {
	mu               sync.Mutex
	snapshotMu       sync.Mutex // to protect snapshot operations
	me               int
	rf               raftapi.Raft
	applyCh          chan raftapi.ApplyMsg
	maxraftstate     int // snapshot if log grows this big
	sm               StateMachine
	idGenerator      IdGenerator
	pendingOps       map[int]*PendingOp // ongoing apply operations indexed by ID
	dead             int32              // set by Kill()
	lastAppliedIndex int                // the last applied index to the state machine
}

func (rsm *RSM) killed() bool {
	z := atomic.LoadInt32(&rsm.dead)
	return z == 1
}

func (rsm *RSM) setKilled() {
	atomic.StoreInt32(&rsm.dead, 1)
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}
func (rsm *RSM) stateMachine() StateMachine {
	return rsm.sm
}

func initRsm(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine, IdGenerator IdGenerator) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		idGenerator:  IdGenerator,
		pendingOps:   make(map[int]*PendingOp),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	return rsm
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	idGen, _ := NewSnowflake(int64(rand.Intn(64)))
	rsm := initRsm(servers, me, persister, maxraftstate, sm, idGen)
	rsm.recoverStatesForSm(persister.ReadSnapshot())
	go rsm.logApplier()
	if maxraftstate > 0 {
		go rsm.snapshotApplier()
	}
	return rsm
}

func (rsm *RSM) recoverStatesForSm(snapshot []byte) {
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
}

func (rsm *RSM) snapshotApplier() {
	for !rsm.killed() && !rsm.Raft().GetKill() {
		rsm.mu.Lock()
		maxSize := rsm.maxraftstate
		rsm.mu.Unlock()
		raftSize := rsm.Raft().PersistBytes()
		if raftSize <= maxSize {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		rsm.snapshotMu.Lock()
		// ask state machine to snapshot
		snapshotData := rsm.stateMachine().Snapshot()
		idx := rsm.getLastAppliedIndex()
		rsm.snapshotMu.Unlock()
		// give the snapshot to raft
		rsm.Raft().Snapshot(idx, snapshotData)
	}
}

func (rsm *RSM) logApplier() {
	for applyMsg := range rsm.applyCh {
		DPrintf("[%d] receive applyMsg from channel with: index %d, commandValid %t, commandIndex %d, command %T \n", rsm.me, applyMsg.CommandIndex, applyMsg.CommandValid, applyMsg.CommandIndex, applyMsg.Command)
		if applyMsg.CommandValid {
			rsm.applyLogEntry(applyMsg)
		} else if applyMsg.SnapshotValid {
			rsm.applySnapshot(applyMsg)
		}
	}
	// When the apply channel is closed, we should also clear the ongoing apply ops list
	rsm.mu.Lock()
	rsm.clearPendingOps()
	rsm.mu.Unlock()
	rsm.setKilled()
}

func (rsm *RSM) applyLogEntry(applyMsg raftapi.ApplyMsg) {
	rsm.snapshotMu.Lock()
	// ask the state machine to apply the command
	stateMachineRetValue := rsm.sm.DoOp(applyMsg.Command.(Op).Req)
	rsm.mu.Lock()
	rsm.lastAppliedIndex = max(rsm.lastAppliedIndex, applyMsg.CommandIndex)
	rsm.snapshotMu.Unlock()
	DPrintf("[%d] rsm finish asking state machine DoOp: index %d, id %d, req %T, retValue %v \n", rsm.me, applyMsg.CommandIndex, applyMsg.Command.(Op).Id, applyMsg.Command.(Op).Req, stateMachineRetValue)
	// lookUp the ongoing operation list to fill in the return value
	pendingOp := rsm.pendingOps[applyMsg.CommandIndex]
	if pendingOp != nil {
		if pendingOp.id != applyMsg.Command.(Op).Id {
			rsm.clearPendingOps()
		} else {
			pendingOp.retValue = stateMachineRetValue
			pendingOp.retErr = rpc.OK
			pendingOp.finished = true
		}
	}
	rsm.mu.Unlock()
}

func (rsm *RSM) applySnapshot(snapshotMsg raftapi.ApplyMsg) {
	rsm.snapshotMu.Lock()
	rsm.stateMachine().Restore(snapshotMsg.Snapshot)
	rsm.snapshotMu.Unlock()

	rsm.mu.Lock()
	rsm.lastAppliedIndex = snapshotMsg.SnapshotIndex
	DPrintf("[%d] rsm-applySnapshot() at : index %d, term %d \n", rsm.me, snapshotMsg.SnapshotIndex, snapshotMsg.SnapshotTerm)
	rsm.mu.Unlock()
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	operation := Op{Me: rsm.me, Id: rsm.idGenerator.NextID(), Req: req}
	DPrintf("[%d] submit operation to Raft with id %d \n", rsm.me, operation.Id)
	rsm.mu.Unlock()
	index, startTerm, isLeader := rsm.Raft().Start(operation)
	if !isLeader {
		DPrintf("[%d] not leader, cannot start operation with id %d \n", rsm.me, operation.Id)
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Lock()
	pendingOp := &PendingOp{
		id:    operation.Id,
		index: index,
	}
	rsm.pendingOps[index] = pendingOp
	DPrintf("[%d] Start waiting for command operation to be applied on idx: index %d, id %d, req %T \n", rsm.me, index, operation.Id, operation.Req)
	rsm.mu.Unlock()

	for !rsm.Raft().GetKill() && !rsm.killed() {
		rsm.mu.Lock()
		if pendingOp.finished {
			DPrintf("[%d] command applied on idx: index %d, id %d, req %T \n", rsm.me, index, pendingOp.id, operation.Req)
			rsm.mu.Unlock()
			break
		}
		currentTerm, _ := rsm.rf.GetState()
		if currentTerm != startTerm {
			rsm.mu.Unlock()
			DPrintf("[%d] current term %d does not match start term %d, operation id %d \n", rsm.me, currentTerm, startTerm, operation.Id)
			// Leadership lost
			return rpc.ErrWrongLeader, nil
		}
		DPrintf("[%d] [in loop] waiting for command operation to be applied on idx: index %d, id %d, req %T \n", rsm.me, index, operation.Id, operation.Req)
		rsm.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	delete(rsm.pendingOps, index)
	return pendingOp.retErr, pendingOp.retValue
}

func (rsm *RSM) clearPendingOps() {
	for _, ongoingOp := range rsm.pendingOps {
		ongoingOp.retErr = rpc.ErrWrongLeader
		ongoingOp.retValue = nil
		ongoingOp.finished = true
	}
}

func (rsm *RSM) getLastAppliedIndex() int {
	return rsm.lastAppliedIndex
}
