package main

// Raft Core RPC Types
type RequestVoteArgs struct {
	Term        int
	CandidateID string
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	MatchIndex int
}

// Cluster API RPCs
type SubmitCommandArgs struct {
	Command string
}

type SubmitCommandReply struct {
	Success  bool
	LeaderID string
	Message  string
}

// Worker heartbeat
type WorkerHeartbeatArgs struct {
	WorkerID string
	Host     string
	Port     int
}

type WorkerHeartbeatReply struct {
	Success  bool
	LeaderID string
	Message  string
}

type ListWorkersReply struct {
	Workers []WorkerInfo
	Leader  string
}
