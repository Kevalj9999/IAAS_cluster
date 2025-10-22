package main

import (
	"sync"
	"time"
)

// RaftRole enumerates node role
type RaftRole string

const (
	Follower  RaftRole = "Follower"
	Candidate RaftRole = "Candidate"
	Leader    RaftRole = "Leader"
)

// LogEntry is a single log entry
type LogEntry struct {
	Index   int
	Term    int
	Command string
}

// PersistentState holds fields that should persist across restarts (simplified)
type PersistentState struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
}

// VolatileState holds in-memory ephemeral state
type VolatileState struct {
	commitIndex int
	lastApplied int
}

// RaftNode is the primary struct for a node
type RaftNode struct {
	mu sync.Mutex

	ID    string
	Port  int
	Peers []string // peer addresses "host:port"

	// states
	role       RaftRole
	persistent PersistentState
	volatile   VolatileState
	leaderID   string
	StateFile  string

	// leader volatile fields (maintained only on leader)
	nextIndex  map[string]int // next index to send to each peer
	matchIndex map[string]int // highest replicated index on each peer

	// timers & channels
	electionTimer  *time.Timer
	grantVoteCh    chan bool
	heartbeatCh    chan bool
	leaderChangeCh chan RaftRole
	stopCh         chan struct{}

	// apply channel for committed entries -> state machine
	applyCh chan LogEntry

	// Worker registry derived from committed log (persisted entries)
	Workers map[string]WorkerInfo
	// Volatile liveness info maintained by the current leader
	WorkerLastSeen map[string]time.Time
	// mutex already exists as n.mu
}

// WorkerInfo holds data persisted in the Raft log (ID, host, port)
type WorkerInfo struct {
	ID   string
	Host string
	Port int
}

// Volatile worker state (not persisted)
type WorkerState struct {
	Info     WorkerInfo
	LastSeen time.Time
	Status   string // e.g., "alive", "dead"
}
