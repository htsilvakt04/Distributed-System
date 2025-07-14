package raft

import (
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"sync"
	"time"
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
	electionTimeout   time.Duration         // timeout for starting a new election
	// volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	// volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on that server
	/* persistent state on all servers */
	logs              []*raftapi.LogEntry // log entries, indexed by log index
	currentTerm       int
	votedFor          int // candidateId that received vote in current term
	lastIncludedIndex int
	lastIncludedTerm  int // term of the last included log entry in the snapshot
	baseIndex         int
}

func (rf *Raft) GetElectionTimeout() time.Duration {
	return rf.electionTimeout
}

func (rf *Raft) SetElectionTimeout(electionTimeout time.Duration) {
	rf.electionTimeout = electionTimeout
}

type EncodedRaftState struct {
	CurrentTerm       int
	VotedFor          int
	Logs              []raftapi.LogEntry
	BaseIndex         int
	LastIncludedIndex int
	LastIncludedTerm  int
}

type EncodedSnapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
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

	Term                 int
	VoteGranted          bool
	RejectReason         string // optional, used for debugging
	LatestCommittedIndex int    // index of the latest committed log entry in the follower's log
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
	XTerm   int // term of the conflicting entry
	XIndex  int // index of the log entry that caused the conflict
	XLen    int // length of the log at the follower, used to update the follower's log
}

type InstallSnapshotRPCArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // snapshot data
}
type InstallSnapshotRPCReply struct {
	Term int // current term of the follower
}
