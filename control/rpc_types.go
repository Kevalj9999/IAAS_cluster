package main

// ==== Raft Core RPC Types ====
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

// ==== Cluster API RPCs ====
type SubmitCommandArgs struct {
	Command string
}

type SubmitCommandReply struct {
	Success  bool
	LeaderID string
	Message  string
}

// ==== Worker heartbeat and list ====
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

// ==== Deployment RPCs (when leader asks a node to host a site) ====
type AssignDeploymentArgs struct {
	User    string
	Site    string
	FileURL string
}

type AssignDeploymentReply struct {
	Success bool
	Message string
}
