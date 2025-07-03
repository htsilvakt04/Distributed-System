package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type State string

const (
	StateFollower  State = "follower"
	StateCandidate State = "candidate"
	StateLeader    State = "leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex            // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd   // RPC end points of all peers
	persister         *tester.Persister     // Object to hold this peer's persisted state
	applyCh           chan raftapi.ApplyMsg // Channel to send apply messages to the service
	me                int                   // this peer's index into peers[]
	dead              int32                 // set by Kill()
	state             State                 // current state of the peer (follower, candidate, leader)
	lastHeartbeat     time.Time             // time of last heartbeat received from leader
	heartbeatInterval time.Duration         // interval for sending heartbeats
	/* persistent state on all servers:
	- currentTerm
	- votedFor
	- logs[]
	*/
	logs        []*raftapi.LogEntry // log entries, indexed by log index
	currentTerm int
	votedFor    int // candidateId that received vote in current term
	// volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	// volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on that server
}

func (rf *Raft) getPeerPrevLogEntryPtr(peerIdx int) *raftapi.LogEntry {
	return rf.logs[rf.nextIndex[peerIdx]-1]
}

func (rf *Raft) getPeerNextLogEntries(peerIdx int) []raftapi.LogEntry {
	if rf.nextIndex[peerIdx] >= len(rf.logs) {
		return []raftapi.LogEntry{}
	}
	entries := rf.logs[rf.nextIndex[peerIdx]:]
	result := make([]raftapi.LogEntry, len(entries))

	for i, entry := range entries {
		result[i] = *entry
	}
	return result
}
func (rf *Raft) GetIsLeader() bool {
	return rf.state == StateLeader
}

func (rf *Raft) SetVotedFor(votedFor int) {
	rf.votedFor = votedFor
}

func (rf *Raft) SetState(state State) {
	rf.state = state
}

func (rf *Raft) SetCurrentTerm(currentTerm int) {
	rf.currentTerm = currentTerm
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) getLastLogEntry() *raftapi.LogEntry {
	return rf.logs[len(rf.logs)-1]
}
func (rf *Raft) getFirstLogEntry() *raftapi.LogEntry {
	return rf.logs[0]
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	LastLogIndex int
	LastLogTerm  int
	CandidateId  int
}
type RequestVoteReply struct {
	/*
		- term: term of the follower
		- voteGranted: whether the follower granted the vote for this candidate
	*/

	Term         int
	VoteGranted  bool
	RejectReason string // optional, used for debugging
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int
	Entries      []raftapi.LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.GetIsLeader()
	if !isLeader {
		DPrintf("[%d] not leader, cannot start command: %v", rf.me, command)
		return index, term, isLeader
	}

	// Create a new log entry, add it to the log, persist the state
	lastRaftLogEntry := rf.getLastLogEntry()
	rf.logs = append(rf.logs, &raftapi.LogEntry{
		Term:    rf.currentTerm,
		Index:   lastRaftLogEntry.Index + 1,
		Command: command,
	})
	// persist the state: todo
	return rf.getLastLogEntry().Index + 1, rf.currentTerm, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Your code here (3A)
func (rf *Raft) ticker() {
	for !rf.killed() {
		start := time.Now()
		// pause for a random amount of time between 300 and 600 milliseconds
		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		// Check if a leader election should be started.
		if rf.state != StateLeader && rf.lastHeartbeat.Before(start) {
			rf.mu.Unlock()
			rf.attemptElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) attemptElection() {
	rf.mu.Lock()
	DPrintf("[%d] attemp election currentTerm: %d", rf.me, rf.currentTerm+1)
	rf.currentTerm++
	lastTerm := rf.currentTerm
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.mu.Unlock()
	votes := 1
	done := false
	start := time.Now()
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		rf.mu.Lock()
		lastLogEntry := rf.getLastLogEntry()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			LastLogIndex: lastLogEntry.Index,
			LastLogTerm:  lastLogEntry.Term,
			CandidateId:  rf.me,
		}
		reply := &RequestVoteReply{}
		rf.mu.Unlock()

		go func() {
			DPrintf("[%d] sendRequestVote to %d, args: %+v", rf.me, peerIdx, args)
			ok := rf.sendRequestVote(peerIdx, args, reply)
			if !ok {
				DPrintf("[%d] failed to send RequestVote to %d", rf.me, peerIdx)
				return
			}

			DPrintf("[%d] sendRequestVote reply from %d, term: %d, voteGranted: %t, reject reson: %s", rf.me, peerIdx, reply.Term, reply.VoteGranted, reply.RejectReason)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() || done || rf.currentTerm != lastTerm || rf.state == StateFollower {
				return
			}

			// convert to follow if cond meet
			if ok := rf.convertToFollower(reply.Term); ok {
				done = true
				return
			}

			if reply.VoteGranted {
				votes++
			}

			// tally the votes
			if winElection := votes > len(rf.peers)/2; winElection {
				DPrintf("[%d] won election, votes: %d, currentTerm: %d", rf.me, votes, rf.currentTerm)
				done = true
				rf.SetState(StateLeader)
				rf.initNextIndex()
				rf.initMatchIndex()
				go rf.heartBeats()
			}
		}()
	}
	DPrintf("[%d] waiting for election to complete", rf.me)
	rf.mu.Lock()
	for !done && time.Since(start) < 300*time.Millisecond {
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
	}
	DPrintf("[%d] election completed, votes: %d, currentTerm: %d", rf.me, votes, rf.currentTerm)
	rf.mu.Unlock()
}

// What happens when a server receives a RequestVote RPC?
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[%d] received RequestVote from %d", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// update the last heartbeat time
	rf.lastHeartbeat = time.Now()
	reply.Term = max(args.Term, rf.currentTerm)
	reply.VoteGranted = false
	raftLastLogEntry := rf.getLastLogEntry()

	if args.Term <= rf.currentTerm {
		DPrintf("[%d] reject vote for %d, term %d is <= currentTerm %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.RejectReason = "sender's term is <= currentTerm"
	} else if rf.convertToFollower(args.Term) {
		reply.VoteGranted = true
		DPrintf("[%d] grant vote for %d, currentTerm: %d", rf.me, args.CandidateId, rf.currentTerm)
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%d] reject vote for %d, already voted for %d", rf.me, args.CandidateId, rf.votedFor)
		reply.RejectReason = "already voted for: " + string(rune(rf.votedFor))
	} else if !upToDateLog(args, raftLastLogEntry) {
		DPrintf("[%d] reject vote for %d, its log is more up-to-date", rf.me, args.CandidateId)
		reply.RejectReason = "candidate's log is not up-to-date"
	} else {
		reply.VoteGranted = true
		DPrintf("[%d] grant vote for %d, currentTerm: %d", rf.me, args.CandidateId, rf.currentTerm)
	}
}

func upToDateLog(args *RequestVoteArgs, raftLastLogEntry *raftapi.LogEntry) bool {
	return !(args.LastLogTerm < raftLastLogEntry.Term || (args.LastLogTerm == raftLastLogEntry.Term && args.LastLogIndex < raftLastLogEntry.Index))
}

// When a server receives an AppendEntries RPC from a leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[%d] received AppendEntries from %d, term: %d", rf.me, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.convertToFollower(args.Term)
	reply.Term = rf.currentTerm
	// stale leader
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.state == StateLeader) {
		DPrintf("[%d] reject AppendEntries from %d, due to stale leader", rf.me, args.LeaderId)
		reply.Success = false
		return
	}
	// update the last heartbeat time
	rf.lastHeartbeat = time.Now()
	// When new leader send RPCs
	if args.PrevLogIndex >= len(rf.logs) {
		DPrintf("[%d] reject AppendEntries from %d, PrevLogIndex %d is out of range", rf.me, args.LeaderId, args.PrevLogIndex)
		reply.Success = false
		return
	}
	// Check if term at PrevLogIndex matches
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] reject AppendEntries from %d, PrevLogTerm %d does not match log entry at PrevLogIndex %d with term %d",
			rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.logs[args.PrevLogIndex].Term)
		reply.Success = false
		return
	}

	/*
		Up to this point we have verified that:
		1. The term of the leader is >= current term.
		2. The PrevLogIndex is within the range of the log.
		3. Entries from PrevLogIndex are consistent
	*/
	// Append the new entries to the log
	for _, entry := range args.Entries {
		// If the entry already exists, we only update it if the term is different\
		if entry.Index <= rf.getLastLogEntry().Index {
			if rf.logs[entry.Index].Term != entry.Term {
				DPrintf("[%d] conflict detected at index %d, truncate log and append new entry", rf.me, entry.Index)
				rf.logs = rf.logs[:entry.Index] // truncate the log at the entry index
				rf.logs = append(rf.logs, &entry)
			}
		} else {
			// If the entry does not exist, we append it to the log
			rf.logs = append(rf.logs, &entry)
		}
	}

	// Update the commit index since the leader's commit index may be higher than us
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogEntry().Index)
	}
	reply.Success = true
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

/*
server: the server to send the RPC to
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

//
//func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
//	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
//	if !ok {
//		DPrintf("[%d] failed to send AppendEntries to %d", rf.me, server)
//		return false
//	}
//
//	DPrintf("[%d] received AppendEntries reply from %d, ok: %t, term: %d", rf.me, server, ok, reply.Term)
//	rf.mu.Lock()
//	rf.convertToFollower(reply.Term)
//	rf.mu.Unlock()
//	return ok
//}

func (rf *Raft) convertToFollower(otherTerm int) bool {
	if otherTerm > rf.currentTerm {
		rf.SetCurrentTerm(otherTerm)
		DPrintf("[%d] convert to follower from state: %s, currentTerm: %d", rf.me, rf.state, rf.currentTerm)
		rf.SetState(StateFollower)
		rf.SetVotedFor(-1)
		return true
	}
	return false
}

func (rf *Raft) heartBeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.GetIsLeader() {
			rf.mu.Unlock()
			return
		}
		peers := rf.peers
		rf.mu.Unlock()
		// send heartbeats to all peers
		for peerIdx := range peers {
			if peerIdx == rf.me {
				continue
			}
			go rf.heartBeat(peerIdx)
		}
		// wait for heartbeat interval before sending the next heartbeat
		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) heartBeat(peerIdx int) {
	rf.mu.Lock()
	DPrintf("[%d] send heartbeat to %d, current term: %d", rf.me, peerIdx, rf.currentTerm)
	prevEntry := rf.getPeerPrevLogEntryPtr(peerIdx)
	lastNextIdx := rf.nextIndex[peerIdx]
	entriesToSend := rf.getPeerNextLogEntries(peerIdx)

	args := &AppendEntriesArgs{
		Term: rf.currentTerm, LeaderId: rf.me,
		PrevLogIndex: prevEntry.Index,
		PrevLogTerm:  prevEntry.Term,
		Entries:      entriesToSend,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()
	// Send the AppendEntries RPC to the peer
	ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf("[%d] failed to send AppendEntries to %d", rf.me, peerIdx)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] received AppendEntries reply from %d, ok: %t, term: %d", rf.me, peerIdx, ok, reply.Term)

	if rf.convertToFollower(reply.Term) {
		return
	}
	// Handle stale heartbeats
	if lastNextIdx != rf.nextIndex[peerIdx] {
		DPrintf("[%d] stale heartbeat reply from %d ignored (nextIndex %d != lastNextIdx %d)", rf.me, peerIdx, rf.nextIndex[peerIdx], lastNextIdx)
		return
	}
	// If the AppendEntries RPC was successful, update the nextIndex and matchIndex for the peer
	if reply.Success {
		if len(entriesToSend) > 0 {
			lastSentIdx := args.PrevLogIndex + len(entriesToSend)
			rf.matchIndex[peerIdx] = lastSentIdx
			rf.nextIndex[peerIdx] = lastSentIdx + 1
		}
	} else {
		// guard against the case where the nextIndex is already at 1
		rf.nextIndex[peerIdx] = max(1, rf.nextIndex[peerIdx]-1)
	}
}

func (rf *Raft) initNextIndex() {
	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
	}
}

func (rf *Raft) initMatchIndex() {
	for i, _ := range rf.peers {
		rf.matchIndex[i] = 0
	}
}

/*
Used by the leader to commit its logs in the background
*/
func (rf *Raft) committer() {
	for !rf.killed() {
		if !rf.GetIsLeader() {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		rf.mu.Lock()
		for N := rf.commitIndex + 1; N < len(rf.logs); N++ {
			count := 1
			// leader only commit logs that under its term
			if rf.logs[N].Term != rf.currentTerm {
				continue
			}

			for peerIdx := range rf.peers {
				if peerIdx != rf.me && rf.matchIndex[peerIdx] >= N {
					count++
				}
			}

			if count <= len(rf.peers)/2 {
				break
			}
			rf.commitIndex = N
		} // end for
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

/*
Used for both leader and followers to apply the logs to its application StateMachine
*/
func (rf *Raft) logApplier(ch chan raftapi.ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.mu.Unlock()
			ch <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Lock()
			DPrintf("[%d] applied log entry at index %d, command: %v", rf.me, rf.lastApplied, rf.logs[rf.lastApplied].Command)
			rf.lastApplied++
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := initRaft(peers, me, persister, applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	// start the log committer goroutine
	go rf.committer()
	// start the apply logs goroutine
	go rf.logApplier(applyCh)
	return rf
}

func initRaft(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.applyCh = applyCh
	rf.persister = persister
	rf.me = me
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.logs = []*raftapi.LogEntry{}
	rf.logs = append(rf.logs, &raftapi.LogEntry{Term: 0, Index: 0}) // initial log entry
	rf.state = StateFollower
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.initNextIndex()
	return rf
}
