package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

func (rf *Raft) getPeerPrevLogEntryPtr(peerIdx int) *raftapi.LogEntry {
	return rf.logs[rf.nextIndex[peerIdx]-1]
}

func (rf *Raft) getPeerNextLogEntries(peerIdx int) []raftapi.LogEntry {
	if rf.nextIndex[peerIdx]-rf.baseIndex >= len(rf.logs) {
		return []raftapi.LogEntry{}
	}
	entries := rf.logs[rf.nextIndex[peerIdx]-rf.baseIndex:]
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
	if len(rf.logs) == 0 {
		return &raftapi.LogEntry{
			Term:  rf.lastIncludedTerm,
			Index: rf.lastIncludedIndex,
		}
	}

	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLogEntry() *raftapi.LogEntry {
	return rf.logs[0]
}

func (rf *Raft) getLogEntryAt(idx int) *raftapi.LogEntry {
	if idx < rf.baseIndex || idx > rf.baseIndex+len(rf.logs) {
		DPrintf("[%d] getLogEntryAt(%d) out of range, baseIndex: %d, logs length: %d", rf.me, idx, rf.baseIndex, len(rf.logs))
		return nil
	}
	if idx == rf.baseIndex+len(rf.logs) {
		if len(rf.logs) == 0 {
			// If the log is empty, return the last included log entry
			return &raftapi.LogEntry{
				Term:  rf.lastIncludedTerm,
				Index: rf.lastIncludedIndex,
			}
		} else {
			return nil
		}
	}
	return rf.logs[idx-rf.baseIndex]
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	raftState := rf.createEncodedRaftState()
	rf.persister.Save(raftState, snapshot)
}

func (rf *Raft) createEncodedRaftState() []byte {
	logs := make([]raftapi.LogEntry, len(rf.logs))
	for i, log := range rf.logs {
		logs[i] = *log
	}
	raftState := EncodedRaftState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Logs:              logs,
		BaseIndex:         rf.baseIndex,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	buff := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buff)
	err := encoder.Encode(raftState)
	if err != nil {
		panic("Encode Error")
		return nil
	}
	return buff.Bytes()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.baseIndex || index >= rf.baseIndex+len(rf.logs) {
		DPrintf("[%d] Snapshot index %d is out of range, baseIndex: %d, logs length: %d", rf.me, index, rf.baseIndex, len(rf.logs))
		return
	}
	lastIncludedTerm := rf.currentTerm
	// truncate the log up to the snapshot index
	logs := make([]*raftapi.LogEntry, 0)
	for i := index - rf.baseIndex + 1; i < len(rf.logs); i++ {
		logs = append(logs, rf.logs[i])
	}
	rf.logs = logs
	rf.baseIndex = index + 1
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.persist(snapshot)
	DPrintf("[%d] Snapshot created at index %d, lastIncludedTerm: %d, baseIndex: %d, logs length: %d", rf.me, index, lastIncludedTerm, rf.baseIndex, len(rf.logs))
	DPrintf("[%d] logs after Snapshot", rf.me)
	for i, entry := range rf.logs {
		DPrintf("[%d] log[%d] = %+v", rf.me, i, *entry)
	}
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
	DPrintf("[%d] is a leader and received start command: %v", rf.me, command)
	// Create a new log entry, add it to the log, persist the state
	nextIdx := rf.baseIndex + len(rf.logs)
	rf.logs = append(rf.logs, &raftapi.LogEntry{
		Term:    rf.currentTerm,
		Index:   nextIdx,
		Command: command,
	})
	rf.persist(nil)
	// print log entries for debugging
	DPrintf("[%d] logs after Start() command", rf.me)
	for i, entry := range rf.logs {
		DPrintf("[%d] log[%d] = %+v", rf.me, i, *entry)
	}
	return nextIdx, rf.currentTerm, isLeader
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
		ms := rf.GetElectionTimeout().Milliseconds() + (rand.Int63() % 300)
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
	rf.persist(nil)
	rf.mu.Unlock()
	votes := 1
	done := false
	start := time.Now()
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		go rf.handleSendRequestVote(peerIdx, &done, lastTerm, &votes)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] received RequestVote from %d", rf.me, args.CandidateId)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	reply.LatestCommittedIndex = rf.commitIndex
	raftLastLogEntry := rf.getLastLogEntry()
	persist := false
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		persist = true
	}

	if args.Term < rf.currentTerm {
		DPrintf("[%d] reject vote for %d, term %d is <= currentTerm %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.RejectReason = "sender's term is <= currentTerm"
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%d] reject vote for %d, already voted for %d", rf.me, args.CandidateId, rf.votedFor)
		reply.RejectReason = "already voted for: " + string(rune(rf.votedFor))
	} else if !IsSenderLogUpToDate(args, raftLastLogEntry) {
		DPrintf("[%d] reject vote for %d, its log is more up-to-date", rf.me, args.CandidateId)
		reply.RejectReason = "candidate's log is not up-to-date"
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		persist = true
		DPrintf("[%d] grant vote for %d, currentTerm: %d", rf.me, args.CandidateId, rf.currentTerm)
	}
	if persist {
		rf.persist(nil)
		DPrintf("[%d] persist state after RequestVote from %d, currentTerm: %d, votedFor: %d", rf.me, args.CandidateId, rf.currentTerm, rf.votedFor)
	}
}

func IsSenderLogUpToDate(args *RequestVoteArgs, raftLastLogEntry *raftapi.LogEntry) bool {
	return !(args.LastLogTerm < raftLastLogEntry.Term || (args.LastLogTerm == raftLastLogEntry.Term && args.LastLogIndex < raftLastLogEntry.Index))
}

// When a server receives an AppendEntries RPC from a leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] received AppendEntries from %d, term: %d, currentTerm: %d, entries %v, PrevLogIndex: %d, LeaderCommit: %d, current commitIndex:%d, is am I leader? %t",
		rf.me, args.LeaderId, args.Term, rf.currentTerm, args.Entries, args.PrevLogIndex, args.LeaderCommit, rf.commitIndex, rf.GetIsLeader(),
	)
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.persist(nil)
	}
	reply.Term = rf.currentTerm
	if !checkLogsCond(args, reply, rf) {
		return
	}

	appendNewEntriesToLogs(args, rf)
	rf.updateCommitIndex(args.LeaderCommit)
	reply.Success = true
	return
}

// Send by the leader to install a snapshot on followers
func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotRPCArgs, reply *InstallSnapshotRPCReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] received InstallSnapshotRPC from %d, term: %d, LastIncludedIndex: %d, LastIncludedTerm: %d",
		rf.me, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	reply.Term = rf.currentTerm
	if rf.handleStaleSnapshot(args) {
		return
	}
	// save the snapshot
	rf.persist(args.Data)
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	if rf.discardLogs(args) {
		return
	}
	rf.baseIndex = args.LastIncludedIndex + 1
	DPrintf("[%d] discarding all logs, set baseIndex to: %d, rf.logs to: %v", rf.me, rf.baseIndex, rf.logs)
	// send snapshot to the application layer
	applyMsg := raftapi.ApplyMsg{
		Snapshot:      args.Data,
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
	rf.mu.Lock()
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	// print logs after snapshot
	DPrintf("[%d] logs after InstallSnapshotRPC, baseIndex: %d", rf.me, rf.baseIndex)
	for i, entry := range rf.logs {
		DPrintf("[%d] log[%d] = %+v", rf.me, i, *entry)
	}
}

func (rf *Raft) sendInstallSnapshotRPC(peerIdx int) {
	rf.mu.Lock()
	DPrintf("[%d] send sendInstallSnapshotRPC() for peer %d, baseIndex: %d, nextIndex: %d", rf.me, peerIdx, rf.baseIndex, rf.nextIndex[peerIdx])
	args, reply := rf.computeInstallSnapshotArgs()

	rf.mu.Unlock()
	ok := rf.peers[peerIdx].Call("Raft.InstallSnapshotRPC", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		DPrintf("[%d] failed to send InstallSnapshotRPC to %d at term: %d, currentTerm: %d", rf.me, peerIdx, args.Term, rf.currentTerm)
		return
	}

	rf.handleInstallSnapshotReply(peerIdx, reply)
}

func (rf *Raft) getPersistedSnapshotData(data []byte) []byte {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return nil
	}
	buff := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buff)

	var snapshot EncodedSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		panic(fmt.Sprintf("Decode Snapshot Error: %v", err))
		return nil
	}

	return snapshot.Data
}

func checkLogsCond(args *AppendEntriesArgs, reply *AppendEntriesReply, rf *Raft) bool {
	// stale leader
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.state == StateLeader) {
		DPrintf("[%d] reject AppendEntries from %d, due to stale leader", rf.me, args.LeaderId)
		reply.Success = false
		return false
	}
	// update the last heartbeat time
	rf.lastHeartbeat = time.Now()
	// When new leader send RPCs
	if args.PrevLogIndex >= rf.baseIndex+len(rf.logs) {
		DPrintf("[%d] reject AppendEntries from %d, PrevLogIndex %d is out of range", rf.me, args.LeaderId, args.PrevLogIndex)
		reply.Success = false
		reply.XLen = rf.baseIndex + len(rf.logs)
		return false
	}
	if args.PrevLogIndex == rf.baseIndex-1 {
		return true
	}
	if args.PrevLogIndex < rf.baseIndex {
		DPrintf("[%d] reject AppendEntries from %d, PrevLogIndex %d is less than baseIndex %d", rf.me, args.LeaderId, args.PrevLogIndex, rf.baseIndex)
		reply.Success = false
		reply.XLen = rf.baseIndex + len(rf.logs)
		return false
	}
	// Check if term at PrevLogIndex matches
	if rf.logs[args.PrevLogIndex-rf.baseIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] reject AppendEntries from %d, PrevLogTerm %d does not match log entry at PrevLogIndex %d with term %d",
			rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.logs[args.PrevLogIndex-rf.baseIndex].Term)
		// Find the first log entry that has same term as PrevLogTerm
		entry := findFirstEntryHasSameTerm(args.PrevLogIndex-rf.baseIndex, rf)
		reply.Success = false
		reply.XIndex = entry.Index
		reply.XTerm = entry.Term
		return false
	}
	return true
}

func appendNewEntriesToLogs(args *AppendEntriesArgs, rf *Raft) {
	DPrintf("[%d] logs before AppendEntries", rf.me)
	for i, entry := range rf.logs {
		DPrintf("[%d] log[%d] = %+v", rf.me, i, *entry)
	}
	persist := false
	for _, entry := range args.Entries {
		// If the entry already exists, we only update it if the term is different
		if entry.Index < rf.baseIndex+len(rf.logs) {
			if rf.logs[entry.Index-rf.baseIndex].Term != entry.Term {
				DPrintf("[%d] conflict detected at index %d, truncate log from index: %d", rf.me, entry.Index, entry.Index)
				logs := make([]*raftapi.LogEntry, 0)
				for i := 0; i < entry.Index-rf.baseIndex; i++ {
					logs = append(logs, rf.logs[i])
				}
				rf.logs = logs
				rf.logs = append(rf.logs, &entry)
				persist = true
			}
		} else {
			// If the entry does not exist, we append it to the log
			rf.logs = append(rf.logs, &entry)
			persist = true
		}
	}

	if persist {
		rf.persist(nil)
	}
	DPrintf("[%d] logs after AppendEntries, with current baseIndex: %d, current commitIdx: %d", rf.me, rf.baseIndex, rf.commitIndex)
	for i, entry := range rf.logs {
		DPrintf("[%d] log[%d] = %+v", rf.me, i, *entry)
	}
}

func findFirstEntryHasSameTerm(fromIdx int, rf *Raft) *raftapi.LogEntry {
	term := rf.logs[fromIdx].Term
	var entry *raftapi.LogEntry
	for i := fromIdx; i >= 0; i-- {
		if rf.logs[i].Term != term {
			break
		} else {
			entry = rf.logs[i]
		}
	}
	return entry
}
func findLastEntryHasSameTerm(term int, rf *Raft) *raftapi.LogEntry {
	var entry *raftapi.LogEntry
	// Find the last log entry that has the same term as reply.XTerm
	for i := len(rf.logs) - 1; i > 0 && rf.logs[i].Term >= term; i-- {
		if rf.logs[i].Term == term {
			entry = rf.logs[i]
			DPrintf("[%d] found log entry with same term %d at index %d", rf.me, term, entry.Index)
			break
		}
	}
	return entry
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

func (rf *Raft) convertToFollower(otherTerm int) {
	rf.SetCurrentTerm(otherTerm)
	DPrintf("[%d] convert to follower from state: %s, currentTerm: %d", rf.me, rf.state, rf.currentTerm)
	rf.SetState(StateFollower)
	rf.SetVotedFor(-1)
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
	if rf.nextIndex[peerIdx] < rf.baseIndex {
		DPrintf("[%d] nextIndex %d is less than baseIndex %d, send InstallSnapshot to %d, nextIdx: %d, baseIdx: %d", rf.me, rf.nextIndex[peerIdx], rf.baseIndex, peerIdx, rf.nextIndex[peerIdx], rf.baseIndex)
		rf.mu.Unlock()
		rf.sendInstallSnapshotRPC(peerIdx)
		return
	}
	args, reply, lastNextIdx := rf.computeHeartbeatArgs(peerIdx)
	rf.mu.Unlock()
	// Send the AppendEntries RPC to the peer
	ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		DPrintf("[%d] failed to send AppendEntries to %d at term: %d, currentTerm: %d", rf.me, peerIdx, args.Term, rf.currentTerm)
		return
	}

	if !rf.handleAppendEntriesReply(peerIdx, lastNextIdx, args, reply) {
		return
	}
}

func (rf *Raft) computeHeartbeatArgs(peerIdx int) (*AppendEntriesArgs, *AppendEntriesReply, int) {
	prevSentIdx, prevSendTerm := rf.getPeerPrevSentEntry(peerIdx)
	DPrintf("[%d] send heartBeat to %d and nextIndex is %d, prevSentIdx: %d, baseIdx: %d, LeaderCommit: %d", rf.me, peerIdx, rf.nextIndex[peerIdx], prevSentIdx, rf.baseIndex, rf.commitIndex)

	entriesToSend := rf.getPeerNextLogEntries(peerIdx)
	// print sent entries for debugging
	DPrintf("[%d] entries to send to %d:", rf.me, peerIdx)
	for _, entry := range entriesToSend {
		DPrintf("[%d] %+v", rf.me, entry)
	}
	args := &AppendEntriesArgs{
		Term: rf.currentTerm, LeaderId: rf.me,
		PrevLogIndex: prevSentIdx,
		PrevLogTerm:  prevSendTerm,
		Entries:      entriesToSend,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	lastNextIdx := prevSentIdx + 1

	return args, reply, lastNextIdx
}

func (rf *Raft) getPeerPrevSentEntry(peerIdx int) (int, int) {
	// If nextIndex is equal to baseIndex, it means we have no logs to send
	if rf.nextIndex[peerIdx] == rf.baseIndex {
		prevSentIdx := rf.baseIndex - 1
		prevSendTerm := rf.lastIncludedTerm
		return prevSentIdx, prevSendTerm
	} else {
		prevSentIdx := rf.nextIndex[peerIdx] - 1
		entry := rf.getLogEntryAt(prevSentIdx)
		if entry == nil {
			panic("getPeerPrevSentEntry: entry is nil, this should not happen")
		}
		return prevSentIdx, entry.Term
	}
}

func (rf *Raft) initNextIndex() {
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogEntry().Index + 1
	}
}

func (rf *Raft) initMatchIndex() {
	for i, _ := range rf.peers {
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) handleAppendEntriesReply(peerIdx int, lastNextIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("[%d] received AppendEntries reply from %d, ok: %t, reply's term: %d, my current term: %d, entries I sent before: %v",
		rf.me, peerIdx, reply.Success, reply.Term, rf.currentTerm, args.Entries)

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		rf.persist(nil)
		return false
	}
	// Handle stale heartbeats
	if lastNextIdx != rf.nextIndex[peerIdx] {
		DPrintf("[%d] stale heartbeat reply from %d ignored (nextIndex %d != lastNextIdx %d)", rf.me, peerIdx, rf.nextIndex[peerIdx], lastNextIdx)
		return false
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			lastSentIdx := args.PrevLogIndex + len(args.Entries)
			rf.matchIndex[peerIdx] = lastSentIdx
			rf.nextIndex[peerIdx] = lastSentIdx + 1
		}
		DPrintf("[%d] AppendEntries to %d succeeded, updated nextIndex: %d, matchIndex: %d, current commitIdx: %d", rf.me, peerIdx, rf.nextIndex[peerIdx], rf.matchIndex[peerIdx], rf.commitIndex)
	} else {
		DPrintf("[%d] AppendEntries to %d failed, reply: %+v, current commit idx: %d", rf.me, peerIdx, reply, rf.commitIndex)
		// handle logs backtracking
		if reply.XLen != 0 {
			rf.nextIndex[peerIdx] = reply.XLen
		} else if reply.XTerm != 0 && reply.XIndex != 0 {
			entry := findLastEntryHasSameTerm(reply.XTerm, rf)
			if entry != nil {
				rf.nextIndex[peerIdx] = entry.Index + 1
			} else {
				rf.nextIndex[peerIdx] = reply.XIndex
			}
		}
	}
	return true
}

func (rf *Raft) computeInstallSnapshotArgs() (*InstallSnapshotRPCArgs, *InstallSnapshotRPCReply) {
	args := &InstallSnapshotRPCArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapshotRPCReply{}
	return args, reply
}

func (rf *Raft) handleInstallSnapshotReply(peerIdx int, reply *InstallSnapshotRPCReply) {
	DPrintf("[%d] InstallSnapshotRPC reply from %d at term: %d, currentTerm: %d", rf.me, peerIdx, reply.Term, rf.currentTerm)
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		rf.persist(nil)
		return
	}
	// update the nextIndex and matchIndex for the peer
	rf.nextIndex[peerIdx] = max(rf.lastIncludedIndex+1, rf.nextIndex[peerIdx])
	rf.matchIndex[peerIdx] = max(rf.lastIncludedIndex, rf.matchIndex[peerIdx])
}

func (rf *Raft) updateCommitIndex(commit int) {
	if commit > rf.commitIndex {
		lastCommitIndex := rf.commitIndex
		rf.commitIndex = min(commit, rf.getLastLogEntry().Index)
		DPrintf("[%d] updated commitIndex to %d, given current commitIdx: %d, and LeaderCommit: %d from AppendEntries",
			rf.me, rf.commitIndex, lastCommitIndex, commit)
	}
}

func (rf *Raft) computeRequestVoteArgs() (*RequestVoteArgs, *RequestVoteReply) {
	lastLogEntry := rf.getLastLogEntry()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
		CandidateId:  rf.me,
	}
	reply := &RequestVoteReply{}
	return args, reply
}

func (rf *Raft) handleStaleSnapshot(args *InstallSnapshotRPCArgs) bool {
	if args.Term < rf.currentTerm {
		DPrintf("[%d] reject InstallSnapshotRPC from %d, due to stale term", rf.me, args.LeaderId)
		return true
	}
	if args.Term > rf.currentTerm && rf.state != StateFollower {
		rf.convertToFollower(args.Term)
	}
	// update heartbeat time
	rf.lastHeartbeat = time.Now()
	if rf.baseIndex > args.LastIncludedIndex {
		DPrintf("[%d] reject InstallSnapshotRPC from %d, baseIndex %d is greater than LastIncludedIndex %d", rf.me, args.LeaderId, rf.baseIndex, args.LastIncludedIndex)
		return true
	}
	return false
}

func (rf *Raft) discardLogs(args *InstallSnapshotRPCArgs) bool {
	logs := make([]*raftapi.LogEntry, 0)
	if args.LastIncludedIndex < rf.baseIndex+len(rf.logs) && args.LastIncludedTerm == rf.logs[args.LastIncludedIndex-rf.baseIndex].Term {
		DPrintf("[%d] InstallSnapshotRPC: discarding logs up to index %d", rf.me, args.LastIncludedIndex)
		// discard all log entries up to and including LastIncludedIndex
		for i := args.LastIncludedIndex - rf.baseIndex + 1; i < len(rf.logs); i++ {
			logs = append(logs, rf.logs[i])
		}
		rf.logs = logs
		rf.baseIndex = args.LastIncludedIndex
		return true
	}
	rf.logs = logs
	return false
}

func (rf *Raft) handleSendRequestVote(peerIdx int, done *bool, lastTerm int, votes *int) {
	rf.mu.Lock()
	lastLogEntry := rf.getLastLogEntry()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
		CandidateId:  rf.me,
	}
	reply := &RequestVoteReply{}
	DPrintf("[%d] sendRequestVote to %d, currentTerm: %d ,args: %+v", rf.me, peerIdx, rf.currentTerm, args)
	rf.mu.Unlock()
	ok := rf.peers[peerIdx].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		DPrintf("[%d] failed to send RequestVote to %d at term: %d, currentTerm: %d", rf.me, peerIdx, args.Term, rf.currentTerm)
		return
	}

	DPrintf("[%d] sendRequestVote reply from %d, reply term: %d, current Term: %d, voteGranted: %t, reject reson: %s", rf.me, peerIdx, reply.Term, rf.currentTerm, reply.VoteGranted, reply.RejectReason)
	if rf.killed() || *done || rf.currentTerm != lastTerm || rf.state == StateFollower {
		DPrintf("[%d] election stopped, either killed or state changed to follower or already done", rf.me)
		return
	}

	// convert to follow if cond meet
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		rf.persist(nil)
		*done = true
		return
	}

	if reply.VoteGranted {
		*votes++
	}

	// tally the votes
	if winElection := *votes > len(rf.peers)/2; winElection {
		DPrintf("[%d] won election, votes: %d, currentTerm: %d, current CommitIdx: %d", rf.me, votes, rf.currentTerm, rf.commitIndex)
		// print logs for debugging
		DPrintf("[%d] logs after winning election", rf.me)
		for i, entry := range rf.logs {
			DPrintf("[%d] log[%d] = %+v", rf.me, i, *entry)
		}
		*done = true
		rf.SetState(StateLeader)
		rf.initNextIndex()
		rf.initMatchIndex()
		go rf.heartBeats()
	}
}

/*
Used by the leader to commit its logs in the background
*/
func (rf *Raft) logCommitter() {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.GetIsLeader() {
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for N := rf.baseIndex + len(rf.logs) - 1; N > rf.commitIndex; N-- {
			// leader only commit logs that under its term
			if rf.logs[N-rf.baseIndex].Term != rf.currentTerm {
				continue
			}

			count := 1
			for peerIdx := range rf.peers {
				if peerIdx != rf.me && rf.matchIndex[peerIdx] >= N {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("[%d] logCommitter commit log entry at index %d, command: %v", rf.me, N, rf.logs[N-rf.baseIndex].Command)
				break
			}
		} // end for
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

/*
Used for both leader and followers to apply the logs to its application StateMachine
*/
func (rf *Raft) logApplier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			DPrintf("[%d] logApplier info lastApplied: %d, commitIndex: %d, baseIndex: %d, logs length: %d", rf.me, rf.lastApplied, rf.commitIndex, rf.baseIndex, len(rf.logs))
			entry := rf.logs[rf.lastApplied+1-rf.baseIndex]
			applyIdx := entry.Index
			applyCmd := entry.Command
			rf.mu.Unlock()

			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      applyCmd,
				CommandIndex: applyIdx,
			}

			rf.mu.Lock()
			rf.lastApplied++
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) recoverFromEncodedRaftState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buff := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buff)

	var raftState EncodedRaftState
	if err := decoder.Decode(&raftState); err != nil {
		DPrintf("ReadPersist Error: %v\n", err)
		return
	}

	// Initialize the Raft instance with the restored state
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] recoverFromEncodedRaftState raftState: %v", rf.me, raftState)
	rf.SetCurrentTerm(raftState.CurrentTerm)
	rf.SetVotedFor(raftState.VotedFor)
	rf.baseIndex = raftState.BaseIndex
	rf.lastIncludedIndex = raftState.LastIncludedIndex
	rf.lastIncludedTerm = raftState.LastIncludedTerm
	rf.commitIndex = raftState.LastIncludedIndex
	rf.lastApplied = raftState.LastIncludedIndex
	rf.logs = make([]*raftapi.LogEntry, len(raftState.Logs))
	for i, log := range raftState.Logs {
		rf.logs[i] = &raftapi.LogEntry{
			Term:    log.Term,
			Index:   log.Index,
			Command: log.Command,
		}
	}
}

func (rf *Raft) recoverFromEncodedSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	buff := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buff)

	var idx int
	if err := decoder.Decode(&idx); err != nil {
		panic(fmt.Sprintf("[%d] recoverFromEncodedSnapshot Error: %v", rf.me, err))
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastIncludedIndex > idx {
		DPrintf("[%d] baseIndex %d is greater than or equal to lastIncludedIndex %d, no need to recover", rf.me, rf.baseIndex, idx)
		return
	}
	DPrintf("[%d] finished recovering from snapshot, lastIncludedIndex: %d, baseIndex: %d", rf.me, rf.lastIncludedIndex, rf.baseIndex)
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
	rf.recoverFromEncodedRaftState(persister.ReadRaftState())
	rf.recoverFromEncodedSnapshot(persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()
	// start the log committer goroutine
	go rf.logCommitter()
	// start the apply logs goroutine
	go rf.logApplier()
	return rf
}

func initRaft(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.applyCh = applyCh
	rf.persister = persister
	rf.me = me
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.SetElectionTimeout(300 * time.Millisecond)
	rf.logs = []*raftapi.LogEntry{}
	// initial log entry
	rf.logs = append(rf.logs, &raftapi.LogEntry{
		Term:  0,
		Index: 0,
	})
	rf.state = StateFollower
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = 1
	}
	return rf
}
