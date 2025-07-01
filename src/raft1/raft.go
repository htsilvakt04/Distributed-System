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
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	condVar           *sync.Cond          // Condition variable to signal state changes
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *tester.Persister   // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	state             State               // current state of the peer (follower, candidate, leader)
	lastHeartbeat     time.Time           // time of last heartbeat received from leader
	heartbeatInterval time.Duration       // interval for sending heartbeats
	/* persistent state on all servers:
	- currentTerm
	- votedFor
	- logs[]
	*/
	logs        []raftapi.LogEntry // log entries, indexed by log index
	currentTerm int
	votedFor    int // candidateId that received vote in current term
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

func (rf *Raft) GetLastLogEntry() raftapi.LogEntry {
	return rf.logs[len(rf.logs)-1]
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
	Term int
}

type AppendEntriesReply struct {
	Term int
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
	//DPrintf("[%d] ticker()", rf.me)
	for rf.killed() == false {
		start := time.Now()
		// pause for a random amount of time between 150 and 500 milliseconds
		ms := 150 + (rand.Int63() % 350)
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
	DPrintf("[%d] attemp election currentTerm: %d", rf.me, rf.currentTerm)
	votes := 1
	done := false
	rf.currentTerm++
	lastTerm := rf.currentTerm
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.mu.Unlock()

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		rf.mu.Lock()
		lastLogEntry := rf.GetLastLogEntry()
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			LastLogIndex: lastLogEntry.Index,
			LastLogTerm:  lastLogEntry.Term,
			CandidateId:  rf.me,
		}
		reply := &RequestVoteReply{}
		rf.mu.Unlock()

		go func() {
			ok := rf.sendRequestVote(rf.me, args, reply)
			if !ok {
				DPrintf("[%d] failed to send RequestVote to %d", rf.me, peerIdx)
				return
			}

			DPrintf("[%d] sendRequestVote reply from %d, term: %d, voteGranted: %t, reject reson: %s", rf.me, peerIdx, reply.Term, reply.VoteGranted, reply.RejectReason)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() || done || rf.currentTerm != lastTerm {
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
				go rf.sendHeartbeats()
			}
		}()
	}
}

// What happens when a server receives a RequestVote RPC?
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[%d] received RequestVote from %d", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = max(args.Term, rf.currentTerm)
	reply.VoteGranted = true

	if success := rf.convertToFollower(reply.Term); success {
		DPrintf("[%d] convert to follower, currentTerm: %d", rf.me, rf.currentTerm)
		reply.VoteGranted = false
		reply.RejectReason = "converted to follower"
		return
	}

	// already voted for someone in this term, reject vote
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%d] reject vote for %d, already voted for %d", rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		reply.RejectReason = "already voted for: " + string(rune(rf.votedFor))
		return
	}

	raftLastLogEntry := rf.GetLastLogEntry()
	// if candidate’s log is not up-to-date as receiver’s log, reject vote
	if args.LastLogTerm < raftLastLogEntry.Term || (args.LastLogTerm == raftLastLogEntry.Term && args.LastLogIndex < raftLastLogEntry.Index) {
		DPrintf("[%d] reject vote for %d, its log is more up-to-date", rf.me, args.CandidateId)
		reply.VoteGranted = false
		reply.RejectReason = "candidate's log is not up-to-date"
		return
	}

	DPrintf("[%d] grant vote for %d, currentTerm: %d", rf.me, args.CandidateId, rf.currentTerm)
}

// What happens when a server receives an AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// update the last heartbeat time
	rf.lastHeartbeat = time.Now()
	rf.convertToFollower(args.Term)
	reply.Term = max(args.Term, rf.currentTerm)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) convertToFollower(otherTerm int) bool {
	if otherTerm > rf.currentTerm {
		DPrintf("[%d] convert to follower, currentTerm: %d", rf.me, rf.currentTerm)
		rf.SetCurrentTerm(otherTerm)
		rf.SetState(StateFollower)
		rf.SetVotedFor(-1)
		return true
	}
	return false
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.GetIsLeader() {
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		peers := rf.peers
		rf.mu.Unlock()
		// send heartbeats to all peers
		for peerIdx := range peers {
			if peerIdx == rf.me {
				continue
			}
			rf.mu.Lock()
			DPrintf("[%d] send heartbeat to %d, current term: %d", rf.me, peerIdx, rf.currentTerm)
			args := &AppendEntriesArgs{Term: rf.currentTerm}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(peerIdx, args, reply)
			rf.mu.Lock()
			rf.convertToFollower(reply.Term)
			rf.mu.Unlock()
		}
	}

	// wait for heartbeat interval before sending the next heartbeat
	time.Sleep(rf.heartbeatInterval)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.condVar = sync.NewCond(&rf.mu)
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.logs = []raftapi.LogEntry{}
	rf.logs = append(rf.logs, raftapi.LogEntry{Term: 0, Index: 0}) // initial log entry
	rf.state = StateFollower
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
