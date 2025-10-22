package main

// RequestVoteArgs and Reply for RAFT voting RPC
type RequestVoteArgs struct {
	Term        int
	CandidateID string
	// LastLogIndex/Term omitted for simplicity
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs and Reply for RAFT heartbeat/log replication
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
	MatchIndex int // index of last matched entry on follower when success
}

// AssignDeploymentArgs defines what leader sends to a worker to deploy a site.
type AssignDeploymentArgs struct {
	User    string
	Site    string
	FileURL string // <-- this is the new field (URL to zip)
}

// AssignDeploymentReply is returned by the worker after trying to deploy.
type AssignDeploymentReply struct {
	Success bool
	Message string
}
